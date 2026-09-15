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
package org.eclipse.daanse.rolap.common.member;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import org.eclipse.daanse.olap.cache.BoundedCache;
import org.junit.jupiter.api.Test;

class IncrementalCollectionCacheTest {

    /**
     * addToEntry is copy-on-write: a reader iterates the collection it
     * already took from the cache, so the entry grows by replacement
     * copier (the reader's reference grew).
     */
    @Test
    void addToEntryCopiesInsteadOfGrowingInPlace() {
        IncrementalCollectionCache<String, Collection<String>> cache =
                new IncrementalCollectionCache<>(BoundedCache.ofEntries(10), TreeSet::new);
        cache.addToEntry("k", new TreeSet<>(List.of("a")));
        Collection<String> reader = cache.get("k");

        cache.addToEntry("k", new TreeSet<>(List.of("b")));

        assertThat(reader)
                .as("the reader's collection must not grow underneath it")
                .containsExactly("a");
        assertThat(cache.get("k")).containsExactlyInAnyOrder("a", "b");
    }

    /**
     * The copier also normalizes an entry a shrink left behind as a plain
     * list (removeMember replaces entries with an ArrayList): the next
     * merge re-sorts and de-duplicates through the TreeSet copy.
     */
    @Test
    void mergeNormalizesThroughTheCopier() {
        IncrementalCollectionCache<String, Collection<String>> cache =
                new IncrementalCollectionCache<>(BoundedCache.ofEntries(10), TreeSet::new);
        // seed a non-TreeSet entry directly through the backing cache -
        // the incremental facade itself only writes through addToEntry
        cache.getCache().put("k", new java.util.ArrayList<>(List.of("b", "a")));

        cache.addToEntry("k", new TreeSet<>(List.of("a", "c")));

        assertThat(cache.get("k")).containsExactly("a", "b", "c");
    }
}
