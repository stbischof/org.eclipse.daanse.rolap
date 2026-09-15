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
package org.eclipse.daanse.rolap.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.eclipse.daanse.olap.common.MapContextConfig;
import org.eclipse.daanse.rolap.common.agg.AggregationManager;
import org.eclipse.daanse.rolap.common.catalog.RolapCatalogCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The aggregated read-side over every cache family's recordStats: the
 * snapshot captures without touching live state, sums the counters it
 * finds and renders a human-readable summary - the seam the OTel
 * exporter will register its gauges on.
 */
class CacheStatsReportTest {

    private AggregationManager manager;

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.shutdown();
        }
    }

    @Test
    void captureSumsWhatItFindsAndFormats() {
        AbstractRolapContext context = mock(AbstractRolapContext.class);
        when(context.getConfig()).thenReturn(new MapContextConfig(() -> Map.of()));
        RolapCatalogCache catalogCache = new RolapCatalogCache(context);
        doReturn(catalogCache).when(context).getCatalogCache();
        manager = new AggregationManager(context);
        doReturn(manager).when(context).getAggregationManager();

        CacheStatsReport.Snapshot snapshot = CacheStatsReport.capture(context);

        assertThat(snapshot.catalogLive()).isNotNull();
        assertThat(snapshot.memberCachesVisited()).isZero();
        assertThat(snapshot.nativeTupleCaches()).isEmpty();
        // the shared manager carries at least the in-memory store worker
        assertThat(snapshot.segmentStores()).isNotEmpty();
        assertThat(snapshot.formatted())
            .contains("cache stats snapshot")
            .contains("catalogs:")
            .contains("memberLists(0 caches)")
            .contains("store ");
        // a second capture is independent and still consistent
        assertThat(CacheStatsReport.capture(context).segmentStores())
            .hasSameSizeAs(snapshot.segmentStores());
    }

    @Test
    void snapshotFieldsAreValueLike() {
        CacheStatsReport.Snapshot snapshot = new CacheStatsReport.Snapshot(
            com.github.benmanes.caffeine.cache.stats.CacheStats.empty(),
            3, 5_000_000, com.github.benmanes.caffeine.cache.stats.CacheStats.empty(),
            0, Map.of(), List.of());
        assertThat(snapshot.formatted()).contains("loads=3").contains("loadMillis=5");
    }
}
