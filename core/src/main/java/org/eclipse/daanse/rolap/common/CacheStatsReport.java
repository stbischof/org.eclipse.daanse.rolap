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

import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.daanse.olap.api.element.Cube;
import org.eclipse.daanse.olap.api.element.Hierarchy;
import org.eclipse.daanse.rolap.api.RolapContext;
import org.eclipse.daanse.rolap.common.agg.SegmentCacheManager;
import org.eclipse.daanse.rolap.common.agg.SegmentCacheStats;
import org.eclipse.daanse.rolap.common.cache.BoundedCache;
import org.eclipse.daanse.rolap.common.catalog.RolapCatalogCache;
import org.eclipse.daanse.rolap.common.member.CachingMemberReader;
import org.eclipse.daanse.rolap.common.member.MemberCacheImpl;
import org.eclipse.daanse.rolap.element.RolapCatalog;
import org.eclipse.daanse.rolap.element.RolapCubeHierarchy;
import org.eclipse.daanse.rolap.element.RolapHierarchy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.stats.CacheStats;

/**
 * One read-only snapshot over every cache family's existing recordStats
 * read-sides - the aggregation the per-cache javadoc has promised as
 * "feeds the planned telemetry export; nothing reads the stats yet".
 * The later OTel exporter registers gauges over exactly this record;
 * until then {@link #capture} logs the summary at debug and the testkit
 * probes can print it.
 *
 * Capture is read-only and lock-free: it walks the LIVE catalogs (the
 * pooled list), each hierarchy's member caches (raw
 * and cube-wrapper level, identity-deduped) and the segment store
 * workers. Numbers are Caffeine/ LongAdder snapshots - consistent per
 * counter, not across counters.
 */
public final class CacheStatsReport {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheStatsReport.class);

    private CacheStatsReport() {
    }

    /** The aggregate; every field is a plain snapshot, never live state. */
    public record Snapshot(
            CacheStats catalogLive,
            long catalogLoads,
            long catalogLoadNanos,
            CacheStats memberLists,
            int memberCachesVisited,
            Map<String, CacheStats> nativeTupleCaches,
            List<SegmentCacheStats> segmentStores) {

        /** Human-readable multi-line summary (debug logs, probe reports). */
        public String formatted() {
            StringBuilder sb = new StringBuilder(256);
            sb.append("cache stats snapshot\n");
            sb.append("  catalogs: live=").append(brief(catalogLive))
              .append(" loads=").append(catalogLoads)
              .append(" loadMillis=").append(catalogLoadNanos / 1_000_000).append('\n');
            sb.append("  memberLists(").append(memberCachesVisited).append(" caches): ")
              .append(brief(memberLists)).append('\n');
            nativeTupleCaches.forEach((name, stats) ->
                sb.append("  native ").append(name).append(": ").append(brief(stats)).append('\n'));
            for (SegmentCacheStats store : segmentStores) {
                sb.append("  store ").append(store).append('\n');
            }
            return sb.toString();
        }

        private static String brief(CacheStats s) {
            return "hits=" + s.hitCount() + " misses=" + s.missCount()
                + " evictions=" + s.evictionCount();
        }
    }

    /** Captures the aggregate and logs it at debug. */
    public static Snapshot capture(RolapContext context) {
        // the cast serves ONLY the stats read-sides below (no SPI exists
        // for them); the catalog walk goes through the SPI listing
        RolapCatalogCache catalogCache = (RolapCatalogCache) context.getCatalogCache();

        CacheStats memberLists = CacheStats.empty();
        int visited = 0;
        Map<String, CacheStats> nativeStats = new LinkedHashMap<>();
        // identity: the shared hierarchy's cache appears behind several
        // cube hierarchies, and one snapshot must count it once
        Set<MemberCacheImpl> seen =
            java.util.Collections.newSetFromMap(new IdentityHashMap<>());

        for (Object pooled : context.getCatalogCache().getCachedCatalogs()) {
            RolapCatalog catalog = (RolapCatalog) pooled;
            // key by catalog KEY, not name: content-identical catalogs
            // under different connections are distinct pools and must not
            // silently sum into one bucket
            catalog.getNativeRegistry().nativeCacheStats().forEach((name, stats) ->
                nativeStats.merge(catalog.getKey() + "/" + name, stats, CacheStats::plus));
            for (Cube cube : catalog.getCubes()) {
                for (Hierarchy hierarchy : cube.getHierarchies()) {
                    if (hierarchy instanceof RolapCubeHierarchy cubeHierarchy) {
                        if (cubeHierarchy.getMemberReader()
                                instanceof RolapCubeHierarchy.RolapCubeHierarchyMemberReader cubeReader
                                && cubeReader.getRolapCubeMemberCache()
                                    instanceof MemberCacheImpl wrapper
                                && seen.add(wrapper)) {
                            memberLists = memberLists.plus(listStats(wrapper));
                            visited++;
                        }
                        RolapHierarchy shared = cubeHierarchy.getRolapHierarchy();
                        if (shared != null && shared.getMemberReader()
                                instanceof CachingMemberReader caching
                                && seen.add(caching.memberCache)) {
                            memberLists = memberLists.plus(listStats(caching.memberCache));
                            visited++;
                        }
                    }
                }
            }
        }

        List<SegmentCacheStats> stores = List.of();
        if (context instanceof org.eclipse.daanse.olap.core.AbstractBasicContext<?> basic
                && basic.getAggregationManager() != null
                && basic.getAggregationManager().getSegmentCacheManager()
                    instanceof SegmentCacheManager segmentCacheManager) {
            stores = segmentCacheManager.getCacheStats();
        }

        Snapshot snapshot = new Snapshot(
            catalogCache.getCacheStats(),
            catalogCache.catalogLoadCount(),
            catalogCache.catalogLoadNanos(),
            memberLists,
            visited,
            nativeStats,
            stores);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(snapshot.formatted());
        }
        return snapshot;
    }

    /** The three weight-bounded list caches of one member cache, summed. */
    private static CacheStats listStats(MemberCacheImpl memberCache) {
        CacheStats sum = CacheStats.empty();
        sum = plusIfBounded(sum, memberCache.mapLevelToMembers.getCache());
        sum = plusIfBounded(sum, memberCache.mapMemberToChildren.getCache());
        sum = plusIfBounded(sum, memberCache.mapParentToNamedChildren.getCache());
        return sum;
    }

    private static CacheStats plusIfBounded(CacheStats sum, Object cache) {
        return cache instanceof BoundedCache<?, ?> bounded
            ? sum.plus(bounded.stats())
            : sum;
    }
}
