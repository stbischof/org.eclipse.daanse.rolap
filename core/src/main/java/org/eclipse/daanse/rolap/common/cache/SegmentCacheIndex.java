/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (c) 2002-2017 Hitachi Vantara.
 * All Rights Reserved.
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

package org.eclipse.daanse.rolap.common.cache;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.eclipse.daanse.olap.api.execution.Execution;
import org.eclipse.daanse.olap.api.execution.GuardedStatement;
import org.eclipse.daanse.olap.spi.SegmentIdentity;
import org.eclipse.daanse.olap.spi.SegmentBody;
import org.eclipse.daanse.olap.spi.SegmentColumn;
import org.eclipse.daanse.olap.spi.SegmentHeader;

/**
 * The per-catalog segment inventory: which segments cover which cells,
 * which are still loading (slots), and which a flush has marked for
 * removal-after-load.
 *
 * NOT "synchronize before use": every mutation and read is ACTOR-
 * CONFINED - callers reach the index only through
 * {@code SegmentCacheManager#execute}'s single actor thread, which is
 * the whole synchronization model (see the cache handbook's map,
 * rule 1). Direct calls from other threads are a design violation, not
 * a locking bug to patch locally.
 */
public interface SegmentCacheIndex {
    /**
     * Identifies the segment headers that contain a given cell.
     *
     * @param identity Segment identity (dimensionality, measure, predicates)
     * @param coordinates Coordinates
     * @return Empty list if not found; never null
     */
    List<SegmentHeader> locate(
        SegmentIdentity identity,
        Map<String, Comparable> coordinates);

    /**
     * Returns a list of segments that can be rolled up to satisfy a given
     * cell request.
     *
     * @param identity Segment identity (dimensionality, measure, predicates)
     * @param coordinates Coordinates
     *
     * @return List of candidates; each element is a list of headers that, when
     * combined using union, are sufficient to answer the given cell request
     */
    List<List<SegmentHeader>> findRollupCandidates(
        SegmentIdentity identity,
        Map<String, Comparable> coordinates);

    /**
     * Finds a list of headers that intersect a given region.
     *
     * This method is used to find out which headers need to be trimmed
     * or removed during a flush.
     *
     * @param key Region key (identity without the compound predicates)
     * @param region Region columns
     * @return List of intersecting headers
     */
    public List<SegmentHeader> intersectRegion(
        SegmentIdentity.RegionKey key,
        SegmentColumn[] region);

    /**
     * Adds a header to the index.
     *
     * @param header Segment header
     * @param loading Whether segment is pending a load from SQL
     */
    void add(
        SegmentHeader header,
        boolean loading);

    /**
     * Updates a header in the index. This is required when some of the
     * excluded regions have changed.
     * @param oldHeader The old header to replace.
     * @param newHeader The new header to use instead.
     */
    public void update(
        SegmentHeader oldHeader,
        SegmentHeader newHeader);

    /**
     * Changes the state of a header from loading to loaded.
     *
     * The segment must have previously been added by calling {@link #add}
     * with {@code loading=true}; the call fills the load slot and releases
     * everyone waiting on it. Data arriving for an unknown header is
     * discarded.
     *
     * @param header Segment header
     * @param body Segment body
     */
    void loadSucceeded(
        SegmentHeader header,
        SegmentBody body);

    /**
     * Notifies the segment index that a segment failed to load, and removes the
     * segment from the index.
     *
     * The segment must have previously been added using {@link #add}
     * with {@code loading=true}; the call fails the load slot, handing the
     * cause to everyone waiting on it.
     *
     * @param header Header
     * @param throwable Load failure, handed to the waiters
     */
    void loadFailed(
        SegmentHeader header,
        Throwable throwable);

    /**
     * Removes a header from the index.
     *
     * @param header Segment header
     */
    void remove(SegmentHeader header);

    /**
     * Prints the state of the cache to the given writer.
     *
     * @param pw Print writer
     */
    void printCacheState(PrintWriter pw);

    /**
     * Returns a future slot for a segment body, if a segment is currently
     * loading, otherwise null. This is the method to use to get segments
     * 'hot out of the oven'.
     *
     * When this method is invoked, the execution instance of the
     * thread is automatically added to the list of clients for the
     * given segment. The calling code is responsible for calling
     * {@link #cancel(Execution)} when it is done with the segments,
     * or else this registration will prevent others from canceling
     * the running SQL statements associated to this segment.
     *
     * @param header Segment header
     * @return Slot, or null
     */
    Future<SegmentBody> getFuture(Execution execution, SegmentHeader header);

    /**
     * This method must remove all registrations as a client
     * for the given execution.
     *
     * This must terminate all SQL activity for any orphaned
     * segments.
     * @param execution The execution to unregister.
     */
    void cancel(Execution execution);

    /**
     * Tells whether or not a given segment is known to this index.
     */
    public boolean contains(SegmentHeader header);

    /**
     * Whether the header is registered AND not flagged for removal after
     * its load: a flush that hits a still-loading header only flags it
     * ({@code removeAfterLoad}), so {@link #contains} stays true until the
     * load completes - callers deciding whether a load still has an
     * interested party (the store-put gate, the loader's abort check) must
     * use THIS probe, or a flushed-away segment's pre-flush body reaches
     * the stores again.
     */
    public boolean isRegistered(SegmentHeader header);

    /**
     * Whether anyone is still waiting for this header's load: it is either
     * registered, or a flush flagged it while peers were already parked on
     * its slot. Those peers are handed the body and only then is the header
     * evicted, so abandoning the load under them fails their queries for an
     * administrative action they had no part in.
     *
     * The loader's abort check asks THIS rather than {@link #isRegistered}: a
     * flagged header with no waiting client is nobody's business and its SQL
     * should stop, but a flagged header with clients still owes them a result.
     * The store-put gate keeps asking {@link #isRegistered} - a flushed body
     * must not reach the external stores even while it is handed to peers.
     */
    public boolean hasInterestedParties(SegmentHeader header);

    /**
     * Allows to link a {@link GuardedStatement} to a segment. This allows
     * the index to cleanup when {@link #cancel(Execution)} is
     * invoked and orphaned segments are left.
     * @param header The segment.
     * @param stmt The guarded SQL statement.
     */
    public void linkSqlStatement(SegmentHeader header, GuardedStatement stmt);

}
