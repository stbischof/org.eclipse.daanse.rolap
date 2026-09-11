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


package org.eclipse.daanse.rolap.common.cache;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.eclipse.daanse.olap.spi.SegmentBody;
import org.eclipse.daanse.olap.spi.SegmentCache;
import org.eclipse.daanse.olap.spi.SegmentHeader;

/**
 * Implementation of {@link SegmentCache} that stores segments
 * in memory.
 *
 * Segments are held via soft references, so the garbage collector can remove
 * them if it sees fit.
 *
 * The one SegmentCache SPI implementation inside the core bundle (every
 * other store lives in its own segmentcache module). Listeners fire
 * SYNCHRONOUSLY on the caller's thread of put/remove, and rename
 * deliberately fires NO events - same-JVM consumers see the rename
 * through the manager's own publication, and this store has no
 * cross-node audience.
 */
public class MemorySegmentCache implements SegmentCache {
    // Use a thread-safe map because the SegmentCache
    // interface requires thread safety.
    private final Map<SegmentHeader, SoftReference<SegmentBody>> map =
        new ConcurrentHashMap<>();
    private final List<SegmentCacheListener> listeners =
        new CopyOnWriteArrayList<>();
    // cleared references drain here, so getSegmentHeaders never reports
    // entries whose body the garbage collector reclaimed
    private final ReferenceQueue<SegmentBody> clearedBodies = new ReferenceQueue<>();

    private volatile boolean closed;

    private static final class BodyReference extends SoftReference<SegmentBody> {
        private final SegmentHeader header;

        BodyReference(SegmentHeader header, SegmentBody body, ReferenceQueue<SegmentBody> queue) {
            super(body, queue);
            this.header = header;
        }
    }

    private void drainClearedBodies() {
        for (Reference<? extends SegmentBody> ref; (ref = clearedBodies.poll()) != null;) {
            map.remove(((BodyReference) ref).header, ref);
        }
    }

    @Override
	public SegmentBody get(SegmentHeader header) {
        drainClearedBodies();
        final SoftReference<SegmentBody> ref = map.get(header);
        if (ref == null) {
            return null;
        }
        final SegmentBody body = ref.get();
        if (body == null) {
            map.remove(header, ref);
        }
        return body;
    }

    public boolean contains(SegmentHeader header) {
        drainClearedBodies();
        final SoftReference<SegmentBody> ref = map.get(header);
        if (ref == null) {
            return false;
        }
        final SegmentBody body = ref.get();
        if (body == null) {
            map.remove(header, ref);
            return false;
        }
        return true;
    }

    @Override
	public List<SegmentHeader> getSegmentHeaders() {
        drainClearedBodies();
        return new ArrayList<>(map.keySet());
    }

    @Override
	public boolean put(final SegmentHeader header, SegmentBody body) {
        assert header != null;
        assert body != null;
        if (closed) {
            return false;
        }
        drainClearedBodies();
        map.put(header, new BodyReference(header, body, clearedBodies));
        if (listeners.isEmpty()) {
            return true;
        }
        fireSegmentCacheEvent(localEvent(header,
            SegmentCacheListener.SegmentCacheEvent.EventType.ENTRY_CREATED));
        return true; // success
    }

    @Override
	public boolean remove(final SegmentHeader header) {
        if (closed) {
            return false;
        }
        drainClearedBodies();
        final boolean result =
            map.remove(header) != null;
        if (result && !listeners.isEmpty()) {
            fireSegmentCacheEvent(localEvent(header,
                SegmentCacheListener.SegmentCacheEvent.EventType.ENTRY_DELETED));
        }
        return result;
    }

    @Override
    public boolean rename(SegmentHeader oldHeader, SegmentHeader newHeader) {
        // fires no SegmentCacheEvents by design: the local worker registers
        // no listener, and the index is updated by the caller
        if (closed) {
            return false;
        }
        drainClearedBodies();
        final SoftReference<SegmentBody> ref = map.get(oldHeader);
        final SegmentBody body = ref == null ? null : ref.get();
        if (body == null) {
            // GC took the body: the old mapping stays for the drain queue
            return false;
        }
        // put before remove: a concurrent reader always sees one of the two
        // headers. A fresh reference: the old one stays registered under the
        // old header in the drain queue.
        map.put(newHeader, new BodyReference(newHeader, body, clearedBodies));
        map.remove(oldHeader, ref);
        return true;
    }

    @Override
	public void tearDown() {
        closed = true;
        map.clear();
        listeners.clear();
    }

    @Override
	public void addListener(SegmentCacheListener listener) {
        listeners.add(listener);
    }

    @Override
	public void removeListener(SegmentCacheListener listener) {
        listeners.remove(listener);
    }


    /** The one shape every local event shares (the sibling stores have
     * the same factory). */
    private static SegmentCache.SegmentCacheListener.SegmentCacheEvent localEvent(
            SegmentHeader header,
            SegmentCacheListener.SegmentCacheEvent.EventType type) {
        return new SegmentCache.SegmentCacheListener.SegmentCacheEvent() {
            @Override
            public boolean isLocal() {
                return true;
            }

            @Override
            public SegmentHeader getSource() {
                return header;
            }

            @Override
            public EventType getEventType() {
                return type;
            }
        };
    }

    protected void fireSegmentCacheEvent(
        SegmentCache.SegmentCacheListener.SegmentCacheEvent evt)
    {
        for (SegmentCacheListener listener : listeners) {
            listener.handle(evt);
        }
    }
}
