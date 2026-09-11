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
 * jhyde, 30 August, 2001
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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.eclipse.daanse.olap.api.agg.OlapAggregationManager;
import org.eclipse.daanse.olap.api.cache.CacheControl;
import org.eclipse.daanse.olap.api.cache.OlapSegmentCacheManager;
import org.eclipse.daanse.olap.api.connection.Connection;
import org.eclipse.daanse.olap.api.result.Scenario;
import org.eclipse.daanse.olap.api.element.OlapElement;
import org.eclipse.daanse.olap.api.execution.ExecutionContext;
import org.eclipse.daanse.olap.common.Util;
import org.eclipse.daanse.olap.key.BitKey;
import org.eclipse.daanse.sql.statement.api.render.RenderedSql;
import org.eclipse.daanse.rolap.api.RolapContext;
import org.eclipse.daanse.rolap.common.CacheControlImpl;
import org.eclipse.daanse.rolap.common.RolapAggregationManager;
import org.eclipse.daanse.rolap.common.aggmatcher.AggStar;
import org.eclipse.daanse.rolap.common.result.GroupingSetsCollector;
import org.eclipse.daanse.rolap.common.star.RolapStar;
import org.eclipse.daanse.rolap.common.star.StarColumnPredicate;
import org.eclipse.daanse.rolap.common.star.StarPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One per context (NOT a singleton): the facade over the segment-cache
 * machinery. Owns the shared {@link SegmentCacheManager}, the weak map of
 * per-connection session OVERLAYS (writeback isolation), builds the
 * {@link org.eclipse.daanse.olap.api.cache.CacheControl} whose flushes
 * fan out over shared manager plus every overlay, and hosts the static
 * SQL-generation entry points (generateSql, findAgg). It manages no
 * {@link Aggregation} instances - those are per-batch throwaways.
 */
public class AggregationManager extends RolapAggregationManager implements OlapAggregationManager{



    private static final Logger LOGGER =
        LoggerFactory.getLogger(AggregationManager.class);

    private final SegmentCacheManager cacheMgr;

    // thread-free per-session overlays; alive only while the session has
    // pending writeback changes, dropped on commit/rollback or close.
    // Weak keys: an abandoned connection cannot pin its overlay (and through
    // it catalog and segment index); a silent GC drop loses only the
    // overlay's local store
    private final Map<Connection, SegmentCacheManager> sessionOverlays =
        Collections.synchronizedMap(new WeakHashMap<>());


    /**
     * Creates the AggregationManager.
 */
    public AggregationManager(RolapContext context) {
        this.cacheMgr = new SegmentCacheManager(context);
    }

    /**
     * Returns the logger.
     *
     * @return Logger
 */
    public final Logger getLogger() {
        return LOGGER;
    }

    /**
     * Loads the segments of one batch: builds the batch's Aggregation,
     * optimizes the column predicates and hands the load to the
     * SegmentLoader.
     *
     * @param cacheMgr Cache manager
     * @param cellRequestCount Number of missed cells that led to this request
     * @param measures Measures to load
     * @param columns this is the CellRequest's constrained columns
     * @param star the requests' star
     * @param constrainedColumnsBitKey the constrained columns
     * @param compoundPredicateList compound member predicates
     * @param predicates Array of constraints on each column
     * @param groupingSetsCollector grouping sets collector
     * @param segmentFutures List of futures into which each statement will
     *     place a list of the segments it has loaded, when it completes
 */
    public static void loadAggregation(
        SegmentCacheManager cacheMgr,
        int cellRequestCount,
        List<RolapStar.Measure> measures,
        RolapStar.Column[] columns,
        RolapStar star,
        BitKey constrainedColumnsBitKey,
        List<StarPredicate> compoundPredicateList,
        StarColumnPredicate[] predicates,
        GroupingSetsCollector groupingSetsCollector,
        List<Future<Map<Segment, SegmentWithData>>> segmentFutures,
        boolean optimizePredicates)
    {
        Aggregation aggregation = new Aggregation(
            star, constrainedColumnsBitKey, compoundPredicateList,
            cacheMgr.getContext().getConfig().maxConstraints());

        // try to eliminate unnecessary constraints
        // for Oracle: prevent an IN-clause with more than 1000 elements
        predicates = aggregation.optimizePredicates(columns, predicates, optimizePredicates);
        aggregation.load(
            cacheMgr, cellRequestCount, columns, measures, predicates,
            groupingSetsCollector, segmentFutures);
    }

    /**
     * Awaits the flush store operations under a bounded overall budget and
     * returns the failure count. Member-edit flushes run while holding
     * MEMBER_CACHE_LOCK, and a hung store must not hold that lock forever -
     * the ops still complete asynchronously, the balance just stops
     * accounting. A FALSE result means "this store held nothing under that
     * id" (workers degrade and log their own store failures) - only an
     * exceptional completion is an unreported failure. The index is already
     * updated, so a failed store op leaves the old entry behind.
     */
    private static int awaitFlushStoreOperations(List<Future<Boolean>> futures,
            CacheControlImpl.FlushDeadline deadline) {
        int failed = 0;
        for (int i = 0; i < futures.size(); i++) {
            try {
                futures.get(i).get(
                    deadline.remainingNanos(),
                    TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failed += futures.size() - i;
                LOGGER.warn("interrupted awaiting flush store operations");
                break;
            } catch (TimeoutException e) {
                LOGGER.warn(
                    "{} of {} flush store operations still pending after "
                    + "the flush deadline budget; they continue asynchronously",
                    futures.size() - i, futures.size());
                break;
            } catch (ExecutionException e) {
                failed++;
                LOGGER.warn("flush store operation failed", e.getCause());
            }
        }
        return failed;
    }

    /**
     * Returns an API with which to explicitly manage the contents of the cache.
     *
     * @param connection Server whose cache to control
     * @param pw Print writer, for tracing
     * @return CacheControl API
 */
    @Override
    public CacheControl getCacheControl(
        Connection connection,
        final PrintWriter pw)
    {
        return new FlushingCacheControl(connection, pw);
    }

    /**
     * The cache-control handed to callers: cell flushes fan out to the
     * SHARED manager first and then to EVERY session overlay (a foreign
     * writeback session otherwise kept serving the flushed region until
     * commit), store waits are deadline-bounded, and tracing goes to the
     * caller's PrintWriter off the actor.
     */
    private final class FlushingCacheControl extends CacheControlImpl {

        private final Connection flushingConnection;
        private final PrintWriter pw;

        private FlushingCacheControl(Connection connection, PrintWriter pw) {
            super(connection);
            this.flushingConnection = connection;
            this.pw = pw;
        }

        @Override
        protected void flushNonUnion(final CellRegion region, final CacheControlImpl.FlushDeadline deadline) {
            // administrative flush: ALWAYS against the shared manager -
            // routed through the session's overlay it was silently
            // invisible to every other session (and to the external
            // stores). The overlay is flushed additionally, and it must
            // run even when the shared flush throws - otherwise the
            // session keeps serving the flushed cells from its overlay.
            // Throwable, not RuntimeException: an AssertionError out of
            // the actor must not skip the overlay flush either - the
            // session would keep serving the flushed cells
            Throwable sharedFailure = null;
            try {
                flushOn(cacheMgr, region, deadline);
            } catch (RuntimeException | Error e) {
                sharedFailure = e;
            }
            // EVERY session overlay, not only the flushing
            // connection's: a foreign writeback session kept
            // serving the flushed region from its overlay until
            // commit. Snapshot under the monitor, flush outside
            // (overlay flushes are memory-only and cheap).
            List<SegmentCacheManager> overlays;
            synchronized (sessionOverlays) {
                overlays = new ArrayList<>(sessionOverlays.values());
            }
            // the peek covers only the race window between the
            // snapshot above and now: synchronizedMap's computeIfAbsent
            // DOES lock the same monitor as the snapshot, but an overlay
            // born after the block exits and before flushOn runs would
            // otherwise miss this flush
            SegmentCacheManager own = peekSegmentCacheManager(flushingConnection);
            if (own != cacheMgr && !overlays.contains(own)) {
                overlays.add(own);
            }
            // per-overlay try: one failing overlay must not skip the
            // remaining ones, and never MASK the shared failure - the
            // administrator must see that the shared stores kept the
            // region; every later failure rides along as suppressed.
            // Throwable, not RuntimeException: an AssertionError out of
            // an overlay used to displace the recorded shared failure.
            Throwable failure = sharedFailure;
            for (SegmentCacheManager overlay : overlays) {
                try {
                    flushOn(overlay, region, deadline);
                } catch (RuntimeException | Error overlayFailure) {
                    if (failure == null) {
                        failure = overlayFailure;
                    } else {
                        failure.addSuppressed(overlayFailure);
                    }
                }
            }
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
        }

        private void flushOn(SegmentCacheManager segmentCacheManager, CellRegion region,
                CacheControlImpl.FlushDeadline deadline) {
            final SegmentCacheManager.FlushResult result =
                segmentCacheManager.execute(
                    new SegmentCacheManager.FlushCommand(
                        ExecutionContext.current(),
                        segmentCacheManager,
                        region,
                        this));
            for (String message : result.traceMessages) {
                // buffered on the actor, printed here: the actor never
                // blocks on the caller's PrintWriter
                trace(message);
            }
            final List<Future<Boolean>> futures =
                new ArrayList<>();
            for (var task : result.tasks) {
                // enqueues the sequenced operation; ordering per segment
                // id happens inside the manager
                futures.add(task.get());
            }
            int failed = awaitFlushStoreOperations(futures, deadline);
            if (failed > 0) {
                LOGGER.warn("{} of {} flush store operations failed; "
                    + "stale entries may remain in external caches until expiry",
                    failed, futures.size());
            }
        }

        @Override
        public void flush(final CellRegion region) {
            if (pw != null) {
                pw.println("Cache state before flush:");
                printCacheState(pw, region);
                pw.println();
            }
            super.flush(region);
            if (pw != null) {
                pw.println("Cache state after flush:");
                printCacheState(pw, region);
                pw.println();
            }
        }

        @Override
        public void trace(final String message) {
            if (pw != null) {
                pw.println(message);
            }
        }

    }

    @Override
	public Object getCellFromCache(CellRequest request) {
        // Only the local (thread/statement) working store answers here; a
        // global-cache match reaches it via the SegmentCacheManager copy.
        final RolapStar.Measure measure = request.getMeasure();
        return measure.getStar().getCellFromCache(request);
    }

    @Override
	public String getDrillThroughSql(
        final DrillThroughCellRequest request,
        final StarPredicate starPredicateSlicer,
        List<OlapElement> fields,
        final boolean countOnly)
    {
        DrillThroughQuerySpec spec =
            new DrillThroughQuerySpec(
                request,
                starPredicateSlicer,
                fields,
                countOnly);
        RenderedSql pair = spec.generateSql();

        if (getLogger().isDebugEnabled()) {
            getLogger().debug(
                "DrillThroughSQL: {}{}" ,pair.sql(), Util.NL);
        }

        return pair.sql();
    }

    /**
     * Generates the query to retrieve the cells for a list of segments.
     * Called by Segment.load.
     *
     * @return A pair consisting of a SQL statement and a list of suggested
     *     types of columns
 */
    public static RenderedSql generateSql(
        GroupingSetsList groupingSetsList,
        List<StarPredicate> compoundPredicateList, boolean useAggregates)
    {
        final RolapStar star = groupingSetsList.getStar();
        BitKey levelBitKey = groupingSetsList.getDefaultLevelBitKey();
        BitKey measureBitKey = groupingSetsList.getDefaultMeasureBitKey();

        // Check if using aggregates is enabled.
        boolean hasCompoundPredicates = false;
        if (compoundPredicateList != null && !compoundPredicateList.isEmpty()) {
            // Do not use Aggregate tables if compound predicates are present.
            hasCompoundPredicates = true;
        }
        if (useAggregates
             && !hasCompoundPredicates)
        {
            final boolean[] rollup = {false};
            AggStar aggStar = findAgg(star, levelBitKey, measureBitKey, rollup);

            if (aggStar != null) {
                // Got a match, hot damn

                if (LOGGER.isDebugEnabled()) {
                    StringBuilder buf = new StringBuilder(256);
                    buf.append("MATCH: ");
                    buf.append(star.getFactTable().getAlias());
                    buf.append(Util.NL);
                    buf.append("   foreign=");
                    buf.append(levelBitKey);
                    buf.append(Util.NL);
                    buf.append("   measure=");
                    buf.append(measureBitKey);
                    buf.append(Util.NL);
                    buf.append("   aggstar=");
                    buf.append(aggStar.getBitKey());
                    buf.append(Util.NL);
                    buf.append("AggStar=");
                    buf.append(aggStar.getFactTable().getName());
                    buf.append(Util.NL);
                    for (AggStar.Table.Column column
                        : aggStar.getFactTable().getColumns())
                    {
                        buf.append("   ");
                        buf.append(column);
                        buf.append(Util.NL);
                    }
                    LOGGER.debug(buf.toString());
                }

                AggQuerySpec aggQuerySpec =
                    new AggQuerySpec(
                        aggStar, rollup[0], groupingSetsList);
                RenderedSql sql = aggQuerySpec.generateSql();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "generateSql: sql={}", sql.sql());
                }

                return sql;
            }

            // No match, fall through and use fact table.
        }

        if (LOGGER.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("NO MATCH : ");
            sb.append(star.getFactTable().getAlias());
            sb.append(Util.NL);
            sb.append("Foreign columns bit key=");
            sb.append(levelBitKey);
            sb.append(Util.NL);
            sb.append("Measure bit key=        ");
            sb.append(measureBitKey);
            sb.append(Util.NL);
            sb.append("Agg Stars=[");
            sb.append(Util.NL);
            for (AggStar aggStar : star.getAggStars()) {
                sb.append(aggStar.toString());
            }
            sb.append(Util.NL);
            sb.append("]");
            LOGGER.debug(sb.toString());
        }


        // Fact table query
        SegmentArrayQuerySpec spec =
            new SegmentArrayQuerySpec(groupingSetsList, compoundPredicateList);

        RenderedSql pair = spec.generateSql();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "generateSql: sql=" + pair.sql());
        }

        return pair;
    }

    /**
     * Finds an aggregate table in the given star which has the desired levels
     * and measures. Returns null if no aggregate table is suitable.
     *
     * If there no aggregate is an exact match, returns a more
     * granular aggregate which can be rolled up, and sets rollup to true.
     * If one or more of the measures are distinct-count measures
     * rollup is possible only in limited circumstances.
     *
     * @param star Star
     * @param levelBitKey Set of levels
     * @param measureBitKey Set of measures
     * @param rollup Out parameter, is set to true if the aggregate is not
     *   an exact match
     * @return An aggregate, or null if none is suitable.
 */
    public static AggStar findAgg(
        RolapStar star,
        final BitKey levelBitKey,
        final BitKey measureBitKey,
        boolean[] rollup)
    {
        // If there is no distinct count measure, isDistinct == false,
        // then all we want is an AggStar whose BitKey is a superset
        // of the combined measure BitKey and foreign-key/level BitKey.
        //
        // On the other hand, if there is at least one distinct count
        // measure, isDistinct == true, then what is wanted is an AggStar
        // whose measure BitKey is a superset of the measure BitKey,
        // whose level BitKey is an exact match and the aggregate table
        // can NOT have any foreign keys.
        if (rollup == null) {
            throw new IllegalArgumentException("rollup should be not null");
        }
        BitKey fullBitKey = levelBitKey.or(measureBitKey);

        // a levelBitKey with all parent bits set; the input stays untouched
        final BitKey expandedLevelBitKey = expandLevelBitKey(star, levelBitKey);

        // The AggStars are already ordered from smallest to largest so
        // we need only find the first one and return it.
        for (AggStar aggStar : star.getAggStars()) {
            // superset match
            if (!aggStar.superSetMatch(fullBitKey)) {
                continue;
            }
            boolean isDistinct = measureBitKey.intersects(
                aggStar.getDistinctMeasureBitKey());

            // The AggStar has no "distinct count" measures so
            // we can use it without looking any further.
            if (!isDistinct) {
                // Need to use SUM if the query levels don't match
                // the agg stars levels, or if the agg star is not
                // fully collapsed.
                rollup[0] = !aggStar.isFullyCollapsed()
                    || aggStar.hasIgnoredColumns()
                    || (levelBitKey.isEmpty()
                    || !aggStar.getLevelBitKey().equals(levelBitKey));
                return aggStar;
            } else if (aggStar.hasIgnoredColumns()) {
                // we cannot safely pull a distinct count from an agg
                // table if ignored columns are present since granularity
                // may not be at the level of the dc measure
                LOGGER.info("{} cannot be used for distinct-count measures since it has unused or ignored columns.",
                    aggStar.getFactTable().getName());
                continue;
            }

            // If there are distinct measures, we can only rollup in limited
            // circumstances.

            // No foreign keys (except when its used as a distinct count
            //   measure).
            // Level key exact match.
            // Measure superset match.

            // Compute the core levels -- those which can be safely
            // rolled up to. For example,
            // if the measure is 'distinct customer count',
            // and the agg table has levels customer_id,
            // then gender is a core level.
            final BitKey distinctMeasuresBitKey =
                measureBitKey.and(aggStar.getDistinctMeasureBitKey());
            final BitSet distinctMeasures = distinctMeasuresBitKey.toBitSet();
            BitKey combinedLevelBitKey = null;
            for (int k = distinctMeasures.nextSetBit(0); k >= 0;
                k = distinctMeasures.nextSetBit(k + 1))
            {
                final AggStar.FactTable.Measure distinctMeasure =
                    aggStar.lookupMeasure(k);
                BitKey rollableLevelBitKey =
                    distinctMeasure.getRollableLevelBitKey();
                if (combinedLevelBitKey == null) {
                    combinedLevelBitKey = rollableLevelBitKey;
                } else {
                    // and() copies on purpose: combinedLevelBitKey starts as
                    // a reference to a measure's own bit key, which published
                    // keys must never mutate
                    combinedLevelBitKey =
                        combinedLevelBitKey.and(rollableLevelBitKey);
                }
            }

            if (aggStar.hasForeignKeys()) {
                // This is a little pessimistic. If the measure is
                // 'count(distinct customer_id)' and one of the foreign keys is
                // 'customer_id' then it is OK to roll up.

                // Some of the measures in this query are distinct count.
                // Get all of the foreign key columns.
                // For each such measure, is it based upon a foreign key.
                // Are there any foreign keys left over. No, can use AggStar.
                BitKey fkBitKey = aggStar.getForeignKeyBitKey().copy();
                for (AggStar.FactTable.Measure measure
                    : aggStar.getFactTable().getMeasures())
                {
                    if (measure.isDistinct()) {
                        if (measureBitKey.get(measure.getBitPosition())) {
                            fkBitKey.clear(measure.getBitPosition());
                        }
                    }
                }
                if (!fkBitKey.isEmpty()) {
                    // there are foreign keys left so we can not use this
                    // AggStar.
                    continue;
                }
            }

            // We can use the expandedLevelBitKey here because
            // presence of parent level columns won't effect granularity,
            // so will still be an allowable agg match
            if (!aggStar.select(
                    expandedLevelBitKey, combinedLevelBitKey, measureBitKey))
            {
                continue;
            }

            if (expandedLevelBitKey.isEmpty()) {
                // We won't be able to resolve a distinct count measure like
                // this. We need to resolve the distinct values but we don't
                // have any levels for which we constraint on. This would
                // result in either a bloated value (non-distinct) or
                // only the first (non-rolled-up) to be returned.
                continue;
            }
            rollup[0] = !aggStar.getLevelBitKey().equals(expandedLevelBitKey);
            return aggStar;
        }
        return null;
    }

    /**
     * Returns a copy of levelBitKey with the bits of all parent columns
     * set. The argument is never mutated — it is typically the batch's
     * shared, published request key (see the BitKey contract).
     */
    private static BitKey expandLevelBitKey(
        RolapStar star, BitKey levelBitKey)
    {
        BitKey expanded = levelBitKey.copy();
        int bitPos = expanded.nextSetBit(0);
        while (bitPos >= 0) {
            expanded = setParentsBitKey(star, expanded, bitPos);
            bitPos = expanded.nextSetBit(bitPos + 1);
        }
        return expanded;
    }

    private static BitKey setParentsBitKey(
        RolapStar star, BitKey levelBitKey, int bitPos)
    {
        RolapStar.Column parent = star.getColumn(bitPos).getParentColumn();
        if (parent == null) {
            return levelBitKey;
        }
        levelBitKey.set(parent.getBitPosition());
        return setParentsBitKey(star, levelBitKey, parent.getBitPosition());
    }

    @Override
    public Runnable orphanCleanup() {
        // delegates to the shared manager's context-free subset; session
        // overlays share its executors and die with their connections
        return cacheMgr.orphanCleanup();
    }

    @Override
    public void shutdown() {
        synchronized (sessionOverlays) {
            for (SegmentCacheManager sessionOverlay : sessionOverlays.values()) {
                sessionOverlay.shutdown();
            }
            sessionOverlays.clear();
        }
        // stops the actor and executors, tears down the local store and
        // detaches external caches without touching their shared content
        cacheMgr.shutdown();
    }


	@Override
	public OlapSegmentCacheManager getSegmentCacheManager(Connection connection) {
		// Writeback isolation must NOT depend on the perf opt-in: with the
		// flag at its default (false), uncommitted session values were
		// published into the SHARED stores under ordinary header ids -
		// cluster-wide via Redis - and survived a rollback. Pending
		// writeback forces the overlay; the flag additionally gives every
		// connection one (its documented meaning).
		boolean isolate = connection != null
				&& (connection.getContext().getConfig().enableSessionCaching()
					|| hasPendingWriteback(connection));
		if (!isolate) {
			// PURE getter: the old reap-on-read side effect tore down
			// another running statement's overlay mid-query (two
			// statements per connection are normal for XMLA sessions) -
			// the reader's composite cache emptied under it and the
			// reloaded values silently came from the restored fact.
			// Overlays are reaped at transaction boundaries
			// (commit/rollback) and on connection close instead.
			return cacheMgr;
		}
		return sessionOverlays.computeIfAbsent(connection,
				c -> SegmentCacheManager.sessionOverlay(cacheMgr));
	}

	private static boolean hasPendingWriteback(Connection connection) {
		Scenario scenario = connection.getScenario();
		return scenario != null && scenario.hasPendingChanges();
	}

	/**
	 * Read-only view for diagnostics, flush routing and cancel sweeps:
	 * the session's overlay if one EXISTS, else the shared manager -
	 * never creates one (unlike the routing getter, which builds an
	 * overlay for a pending-writeback connection on first use).
	 */
	@Override
	public SegmentCacheManager peekSegmentCacheManager(Connection connection) {
		SegmentCacheManager sessionOverlay =
				connection == null ? null : sessionOverlays.get(connection);
		return sessionOverlay != null ? sessionOverlay : cacheMgr;
	}

	@Override
	public void removeSegmentCacheManager(Connection connection) {
		SegmentCacheManager sessionOverlay = sessionOverlays.remove(connection);
		if (sessionOverlay != null) {
			sessionOverlay.shutdown();
		}
	}

    @Override
    public OlapSegmentCacheManager getSegmentCacheManager() {
        return this.cacheMgr;
    }
}
