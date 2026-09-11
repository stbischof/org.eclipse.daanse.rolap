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
package org.eclipse.daanse.rolap.common.cache;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.ToLongBiFunction;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * A {@link SimpleCache} bounded by weight or entry count (Caffeine, strong
 * references, LRU-like eviction). Suited for values whose cost is their
 * size — member lists, tuple lists — where a cap keeps memory predictable.
 * recordStats feeds CacheStatsReport (and, optionally, the periodic
 * stats logger in BasicContext).
 *
 * <p>
 * There is no global lock:
 * {@link #execute(Task)} iterates the live map weakly consistently, with
 * per-entry atomicity only. Callers that scan to invalidate get that
 * guarantee; callers needing a frozen snapshot must not use this class.
 */
public final class BoundedCache<K, V> implements SimpleCache<K, V> {

    private final Cache<K, V> cache;

    private BoundedCache(Cache<K, V> cache) {
        this.cache = cache;
    }

    /** A cache holding at most {@code maxEntries} entries. */
    // executor(Runnable::run) in both factories: eviction and removal run
    // SYNCHRONOUSLY on the writing thread. Deliberate - deterministic
    // tests and no background threads per cache - at the price of the
    // occasional eviction work on a query thread.
    public static <K, V> BoundedCache<K, V> ofEntries(long maxEntries) {
        return new BoundedCache<>(Caffeine.newBuilder()
                .maximumSize(maxEntries)
                .executor(Runnable::run)
                .recordStats()
                .build());
    }

    /** A cache bounded by the sum of {@code weigher} over its entries. */
    public static <K, V> BoundedCache<K, V> weighted(long maxWeight, ToLongBiFunction<K, V> weigher) {
        return new BoundedCache<>(Caffeine.newBuilder()
                .maximumWeight(maxWeight)
                .<K, V>weigher((k, v) -> (int) Math.min(Integer.MAX_VALUE, Math.max(0, weigher.applyAsLong(k, v))))
                .executor(Runnable::run)
                .recordStats()
                .build());
    }

    @Override
    public V put(K key, V value) {
        Objects.requireNonNull(value, "use remove(key) instead of put(key, null)");
        return cache.asMap().put(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        Objects.requireNonNull(value, "use remove(key) instead of put(key, null)");
        return cache.asMap().putIfAbsent(key, value);
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remapping) {
        return cache.asMap().merge(key, value, remapping);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        Objects.requireNonNull(oldValue, "oldValue");
        Objects.requireNonNull(newValue, "use remove(key) instead of replacing with null");
        // Caffeine re-weighs the replaced value
        return cache.asMap().replace(key, oldValue, newValue);
    }

    @Override
    public V get(K key) {
        return cache.getIfPresent(key);
    }

    @Override
    public V remove(K key) {
        return cache.asMap().remove(key);
    }

    @Override
    public void clear() {
        cache.invalidateAll();
    }

    @Override
    public int size() {
        return cache.asMap().size();
    }

    @Override
    public void execute(Task<K, V> task) {
        task.execute(iterator());
    }

    private Iterator<Entry<K, V>> iterator() {
        return cache.asMap().entrySet().iterator();
    }

    /** Runs any pending eviction now; eviction is otherwise amortized. */
    /** Forces pending maintenance; package-private test seam only. */
    void cleanUp() {
        cache.cleanUp();
    }

    /** Read side of recordStats, for telemetry and the cache-usage probe. */
    public CacheStats stats() {
        return cache.stats();
    }
}
