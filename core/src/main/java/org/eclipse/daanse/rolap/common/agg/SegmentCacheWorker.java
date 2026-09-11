/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
 *
 * ---- All changes after Fork in 2023 ------------------------
 *
 * Project: Eclipse daanse
 *
 * Copyright (c) 2023 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors after Fork in 2023:
 *   SmartCity Jena - initial
 */


package org.eclipse.daanse.rolap.common.agg;

import java.util.List;

import org.eclipse.daanse.olap.spi.SegmentBody;
import org.eclipse.daanse.olap.spi.SegmentCache;
import org.eclipse.daanse.olap.spi.SegmentHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to interact with the {@link SegmentCache}.
 *
 * Fault-isolating wrapper around one {@link SegmentCache}: every call
 * catches Throwable, counts the error and degrades to a MISS/false - a
 * dead store never breaks a query or blocks the pools. checkThread
 * enforces (hard, not assert) that no store I/O ever runs on the cache
 * manager's actor thread: the actor must never wait on a store.
 *
 * @see SegmentCache
 */
public final class SegmentCacheWorker {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(SegmentCacheWorker.class);
    final SegmentCache cache;
    final SegmentCacheStats stats;
    private final Thread cacheMgrThread;

    /** The store's class name, used by the cache-explain audit lines. */
    String cacheName() {
        return cache.getClass().getSimpleName();
    }
    // set when the cache is detached; in-flight calls degrade to a miss
    // instead of reaching a cache the provider may already have closed
    private volatile boolean closing;
    private static final String SEGMENT_CACHE_FAILED_TO_DELETE_SEGMENT =
        "An exception was encountered while deleting a segment from the SegmentCache.";
    private static final String SEGMENT_CACHE_FAILED_TO_SCAN_SEGMENTS =
        "An exception was encountered while getting a list of segment headers in the SegmentCache.";

    /**
     * Creates a worker.
     *
     * @param cache Cache managed by this worker
     * @param cacheMgrThread Thread that the cache manager actor is running on,
     *                       and which therefore should not be used for
     *                       potentially long-running calls this this cache.
     *                       Pass null if methods can be called from any thread.
     */
    public SegmentCacheWorker(SegmentCache cache, Thread cacheMgrThread) {
        this.cache = cache;
        this.stats = new SegmentCacheStats(cache.getClass().getSimpleName());
        this.cacheMgrThread = cacheMgrThread;

        LOGGER.debug(
            "Segment cache initialized: "
            + cache.getClass().getName());
    }

    /**
     * Returns a segment body corresponding to a header.
     *
     * If no cache is configured or there is an error while
     * querying the cache, null is returned none the less.
     *
     * @param header Header to search.
     * @return Either a segment body object or null if there
     * was no cache configured or no segment could be found
     * for the passed header.
     */
    public SegmentBody get(SegmentHeader header) {
        checkThread();
        if (closing) {
            return null;
        }
        try {
            SegmentBody body = cache.get(header);
            stats.recordGet(body != null);
            return body;
        } catch (Throwable t) {
            stats.recordError();
            LOGGER.warn("segment cache get failed; treated as a miss", t);
            return null;
        }
    }

    /**
     * Places a segment in the cache. A refused or failed write degrades to
     * a warning — the cache is optional and the segment stays available
     * from its loader.
     *
     * @param header A header to search for in the segment cache.
     * @param body The segment body to cache.
     */
    public void put(SegmentHeader header, SegmentBody body) {
        checkThread();
        if (closing) {
            return;
        }
        try {
            if (cache.put(header, body)) {
                stats.recordPut();
            } else {
                LOGGER.warn("segment cache refused a put; entry not stored");
            }
        } catch (Throwable t) {
            stats.recordError();
            LOGGER.warn("segment cache put failed; entry not stored", t);
        }
    }

    /**
     * Removes a segment from the cache.
     *
     * @param header A header to remove in the segment cache.
     * @return Whether a segment was removed
     */
    public boolean remove(SegmentHeader header) {
        checkThread();
        if (closing) {
            return false;
        }
        try {
            boolean removed = cache.remove(header);
            if (removed) {
                stats.recordRemove();
            }
            return removed;
        } catch (Throwable t) {
            stats.recordError();
            LOGGER.warn(SEGMENT_CACHE_FAILED_TO_DELETE_SEGMENT, t);
            return false;
        }
    }

    /**
     * Returns a list of segments present in the cache.
     *
     * @return List of headers in the cache
     */
    public List<SegmentHeader> getSegmentHeaders() {
        checkThread();
        if (closing) {
            return List.of();
        }
        try {
            return cache.getSegmentHeaders();
        } catch (Throwable t) {
            stats.recordError();
            LOGGER.warn(SEGMENT_CACHE_FAILED_TO_SCAN_SEGMENTS, t);
            return List.of();
        }
    }

    /** One star's headers; degrades to empty like the full listing. */
    public List<SegmentHeader> getSegmentHeaders(
            org.eclipse.daanse.olap.util.ByteString schemaChecksum,
            String rolapStarFactTableName) {
        checkThread();
        if (closing) {
            return List.of();
        }
        try {
            return cache.getSegmentHeaders(schemaChecksum, rolapStarFactTableName);
        } catch (Throwable t) {
            stats.recordError();
            LOGGER.warn(SEGMENT_CACHE_FAILED_TO_SCAN_SEGMENTS, t);
            return List.of();
        }
    }

    /** Moves a segment to a shrunk header; a failure degrades to false. */
    public boolean rename(SegmentHeader oldHeader, SegmentHeader newHeader) {
        checkThread();
        if (closing) {
            return false;
        }
        try {
            return cache.rename(oldHeader, newHeader);
        } catch (Throwable t) {
            stats.recordError();
            LOGGER.warn("segment cache rename failed", t);
            return false;
        }
    }

    /** Detaches the worker; calls degrade to a miss. */
    public void markClosing() {
        closing = true;
    }

    public void shutdown() {
        checkThread();
        closing = true;
        cache.tearDown();
    }

    private void checkThread() {
        // cache calls are potentially slow and must never run on the
        // cache manager thread
        if (cacheMgrThread == Thread.currentThread()) {
            throw new IllegalStateException(
                "cache call must not run on the cache manager thread " + cacheMgrThread);
        }
    }
}
