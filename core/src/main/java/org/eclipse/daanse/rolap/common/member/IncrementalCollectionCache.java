/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2001-2005 Julian Hyde
 * Copyright (C) 2005-2017 Hitachi Vantara and others
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

package org.eclipse.daanse.rolap.common.member;

import java.util.Collection;
import java.util.function.UnaryOperator;

import org.eclipse.daanse.olap.cache.SimpleCache;

/**
 * Uses a SimpleCache to store a collection of values.
 * Supplements put operations with an "addToEntry", which
 * supports incrementally adding to the collection associated
 * with key.
 *
 * The merge function handed to addToEntry MUST be pure (copy-on-write,
 * no mutation of the existing value): Caffeine may retry the remapping
 * under contention, and an impure merge applied twice corrupts the
 * entry. The copier normalizes both sides before every merge.
 */
public class IncrementalCollectionCache<K, V extends Collection> {
    final SimpleCache<K, V> cache;
    /** Copies an entry before growing it; see {@link #addToEntry}. */
    private final UnaryOperator<V> copier;

    public IncrementalCollectionCache(SimpleCache<K, V> cache, UnaryOperator<V> copier) {
        this.cache = cache;
        this.copier = copier;
    }

    public V get(K key) {
        return cache.get(key);
    }

    public void clear() {
        cache.clear();
    }

    @SuppressWarnings("unchecked")
    public void addToEntry(final K key, final V value) {
        // the value the caller hands in becomes the entry on a miss, so it
        // must already be the collection type the entry should keep. A
        // weight-bounded cache re-weighs the merged value.
        //
        // Copy-on-write: a reader iterates the collection it already took
        // from the cache - growing the entry in place raced that reader
        // into a ConcurrentModificationException. The merge function must
        // also stay pure (a contended merge may retry it): never mutate
        // `existing` here.
        cache.merge(key, value, (existing, incoming) -> {
            V merged = copier.apply(existing);
            merged.addAll(incoming);
            return merged;
        });
    }

    public SimpleCache<K, V> getCache() {
        return cache;
    }

}
