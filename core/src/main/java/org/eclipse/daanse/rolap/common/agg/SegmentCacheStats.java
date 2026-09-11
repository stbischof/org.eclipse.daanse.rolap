/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *   Stefan Bischof (bipolis.org) - initial
 */
package org.eclipse.daanse.rolap.common.agg;

import java.util.concurrent.atomic.LongAdder;

/**
 * Live counters of one segment cache worker: gets (and how many hit),
 * successful puts, successful removes and errors. Read at any time; a
 * telemetry exporter samples these, the engine never resets them.
 */
public final class SegmentCacheStats {

    private final String cacheName;
    private final LongAdder gets = new LongAdder();
    private final LongAdder hits = new LongAdder();
    private final LongAdder puts = new LongAdder();
    private final LongAdder removes = new LongAdder();
    private final LongAdder errors = new LongAdder();

    SegmentCacheStats(String cacheName) {
        this.cacheName = cacheName;
    }

    void recordGet(boolean hit) {
        gets.increment();
        if (hit) {
            hits.increment();
        }
    }

    void recordPut() {
        puts.increment();
    }

    void recordRemove() {
        removes.increment();
    }

    void recordError() {
        errors.increment();
    }

    public String cacheName() {
        return cacheName;
    }

    public long gets() {
        return gets.sum();
    }

    public long hits() {
        return hits.sum();
    }

    public long puts() {
        return puts.sum();
    }

    public long errors() {
        return errors.sum();
    }

    public long removes() {
        return removes.sum();
    }

    @Override
    public String toString() {
        return cacheName + "[gets=" + gets() + " hits=" + hits() + " puts=" + puts() + " removes=" + removes()
            + " errors=" + errors() + "]";
    }
}
