/*
 * Copyright (c) 2022 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *   SmartCity Jena - initial
 *   Stefan Bischof (bipolis.org) - initial
 */
package org.eclipse.daanse.rolap.common.result;

import org.eclipse.daanse.olap.spi.SegmentColumn;
import static org.eclipse.daanse.rolap.common.util.SqlExpressionResolver.genericSql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.SortedSet;
import java.util.concurrent.Future;

import org.eclipse.daanse.rolap.common.sql.SqlQueryCapabilities;
import org.eclipse.daanse.olap.api.cache.CacheCommand;
import org.eclipse.daanse.olap.api.cache.OlapSegmentCacheManager;
import org.eclipse.daanse.olap.api.execution.ExecutionContext;
import org.eclipse.daanse.olap.common.Util;
import org.eclipse.daanse.olap.key.BitKey;
import org.eclipse.daanse.olap.spi.SegmentBody;
import org.eclipse.daanse.olap.spi.SegmentHeader;
import org.eclipse.daanse.olap.spi.SegmentPredicate;
import org.eclipse.daanse.olap.spi.SegmentIdentity;
import org.eclipse.daanse.rolap.common.EnumConvertor;
import org.eclipse.daanse.rolap.common.agg.AggregationManager;
import org.eclipse.daanse.rolap.common.agg.CellRequest;
import org.eclipse.daanse.rolap.common.agg.ListColumnPredicate;
import org.eclipse.daanse.rolap.common.agg.LiteralStarPredicate;
import org.eclipse.daanse.rolap.common.agg.Segment;
import org.eclipse.daanse.rolap.common.agg.SegmentBuilder;
import org.eclipse.daanse.rolap.common.agg.SegmentCacheManager;
import org.eclipse.daanse.rolap.common.agg.SegmentCacheManager.SegmentCacheIndexRegistry;
import org.eclipse.daanse.rolap.common.agg.SegmentLoader;
import org.eclipse.daanse.rolap.common.agg.SegmentWithData;
import org.eclipse.daanse.rolap.common.agg.ValueColumnPredicate;
import org.eclipse.daanse.rolap.common.aggmatcher.AggGen;
import org.eclipse.daanse.rolap.common.aggmatcher.AggStar;
import org.eclipse.daanse.rolap.common.cache.SegmentCacheIndex;
import org.eclipse.daanse.rolap.common.cache.SegmentCacheIndexImpl;
import org.eclipse.daanse.rolap.common.star.RolapSqlExpression;
import org.eclipse.daanse.rolap.common.star.BitKeyExplain;
import org.eclipse.daanse.rolap.common.star.RolapStar;
import org.eclipse.daanse.rolap.common.star.StarColumnPredicate;
import org.eclipse.daanse.rolap.common.star.StarPredicate;
import org.eclipse.daanse.rolap.element.RolapCube;
import org.eclipse.daanse.rolap.element.RolapVirtualCube;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Context for processing a request to the cache manager for segments matching a
 * collection of cell requests. All methods except the constructor are executed
 * by the cache manager's dedicated thread.
 */
public class BatchLoader {
    private static final Logger LOGGER =
        LoggerFactory.getLogger(BatchLoader.class);

    private final ExecutionContext executionContext;
    private final SegmentCacheManager cacheMgr;
    private final SqlQueryCapabilities capabilities;
    private final RolapCube cube;

    private final Map<BatchKey, Batch> batches =
        new HashMap<>();

    /** Headers we intend to serve from cache, grouped by segment identity. */
    private final Map<SegmentIdentity, List<SegmentHeader>> cacheHeaders =
        new LinkedHashMap<>();

    private final Map<SegmentHeader, Future<SegmentBody>> futures =
        new HashMap<>();

    private final List<RollupInfo> rollups = new ArrayList<>();

    private final Set<BitKey> rollupBitmaps = new HashSet<>();

    private final Map<SegmentIdentity.FactKey, SegmentBuilder.SegmentConverter> converterMap =
        new HashMap<>();

    public BatchLoader(
        ExecutionContext executionContext,
        OlapSegmentCacheManager cacheMgr,
        SqlQueryCapabilities capabilities,
        RolapCube cube)
    {
        this.executionContext = executionContext;
        this.cacheMgr = (SegmentCacheManager)cacheMgr;
        this.capabilities = capabilities;
        this.cube = cube;
    }

    public final boolean shouldUseGroupingFunction() {
        return cube.getCatalog().getInternalConnection().getContext()
                .getConfig().enableGroupingSets()
            && capabilities.groupingSets();
    }

    private void recordCellRequest2(final CellRequest request) {
        // If there is a segment matching these criteria, write it to the list
        // of found segments, and remove the cell request from the list.
        final BatchKey key = new BatchKey(request);

        boolean success =
            loadFromCaches(request, key);
        // Skip the batch if we already have a rollup for it.
        if (rollupBitmaps.contains(request.getConstrainedColumnsBitKey())) {
            return;
        }

        // As a last resort, we load from SQL.
        if (!success) {
            if (BitKeyExplain.enabled()) {
                BitKeyExplain.EXPLAIN.debug("no cached segment serves {} -> SQL",
                    BitKeyExplain.explain(request.getMappedCellValues()));
            }
            loadFromSql(request, key);
        }
    }

    /**
     * Loads a cell from caches. If the cell is successfully loaded,
     * we return true.
     */
    private boolean loadFromCaches(
        final CellRequest request,
        final BatchKey key)
    {
        if (cacheMgr.getContext().getConfig().disableCaching()) {
            // Caching is disabled. Return always false.
            return false;
        }
        final RolapStar.Measure requestMeasure = request.getMeasure();
        if (!requestMeasure.getStar().getCatalog().isCellCachingEnabled(requestMeasure.getCubeName())) {
            // cells=off for this cube: neither read nor write its segments
            return false;
        }

        // Is request matched by one of the headers we intend to load?
        final Map<String, Comparable> mappedCellValues =
            request.getMappedCellValues();

        final List<SegmentHeader> sameIdentity =
            cacheHeaders.get(request.segmentIdentity());
        if (sameIdentity != null) {
            for (SegmentHeader header : sameIdentity) {
                if (SegmentCacheIndexImpl.matchesCoordinates(
                        header, mappedCellValues))
                {
                    // It's likely that the header will be in the cache, so this
                    // request will be satisfied. If not, the header will be
                    // removed from the segment index, and we'll be back.
                    return true;
                }
            }
        }
        final RolapStar.Measure measure = request.getMeasure();
        final RolapStar star = measure.getStar();
        final SegmentCacheIndex index =
            ((SegmentCacheIndexRegistry)cacheMgr.getIndexRegistry()).getIndex(star);
        final SegmentIdentity identity =
            request.segmentIdentity();
        final List<SegmentHeader> headersInCache =
            index.locate(identity, mappedCellValues);

        // Ask for the first segment to be loaded from cache. (If it's no longer
        // in cache, we'll be back, and presumably we'll try the second
        // segment.)

        if (!headersInCache.isEmpty()) {
            converterMap.put(
                SegmentCacheIndexImpl.makeConverterKey(request),
                new SegmentBuilder.StarSegmentConverter(
                    measure, key.getCompoundPredicateList()));
            for (SegmentHeader headerInCache : headersInCache) {
                final Future<SegmentBody> future =
                    index.getFuture(executionContext.getExecution(), headerInCache);

                if (future != null) {
                    // Segment header is in cache, body is being loaded.
                    // Worker will need to wait for load to complete.
                    futures.put(headerInCache, future);
                } else {
                    // Segment is in cache.
                    cacheHeaders
                        .computeIfAbsent(headerInCache.identity(), k -> new ArrayList<>())
                        .add(headerInCache);
                }
            }
            if (BitKeyExplain.enabled()) {
                BitKeyExplain.EXPLAIN.debug("serve {} from cached segment {}",
                    BitKeyExplain.explain(mappedCellValues),
                    BitKeyExplain.explain(headersInCache.get(0)));
            }
            return true;
        }

        // Try to roll up if the measure's rollup aggregator supports
        // "fast" aggregation from raw objects.
        //
        // Do not try to roll up if this request has already chosen a rollup
        // with the same target dimensionality. It is quite likely that the
        // other rollup will satisfy this request, and it's complicated to be
        // 100% sure. If we're wrong, we'll be back.

        // Also make sure that we don't try to rollup a measure which
        // doesn't support rollup from raw data, like a distinct count
        // for example. Both the measure's aggregator and its rollup
        // aggregator must support raw data aggregation. We call
        // Aggregator.supportsFastAggregates() to verify.
        Boolean enableInMemoryRollup = cube.getCatalog().getInternalConnection().getContext()
                .getConfig().enableInMemoryRollup();
        if (enableInMemoryRollup
            && measure.getAggregator().supportsFastAggregates(
                    EnumConvertor.toDataTypeJdbc(measure.getDatatype()))
            && measure.getAggregator().getRollup().supportsFastAggregates(
                    EnumConvertor.toDataTypeJdbc(measure.getDatatype()))
            && !isRequestCoveredByRollups(request))
        {
            // Don't even bother doing a segment lookup if we can't
            // rollup that measure.
            final List<List<SegmentHeader>> rollup =
                index.findRollupCandidates(identity, mappedCellValues);
            if (!rollup.isEmpty()) {
                if (BitKeyExplain.enabled()) {
                    BitKeyExplain.EXPLAIN.debug(
                        "serve {} by ROLLING UP {} candidate set(s) in memory",
                        BitKeyExplain.explain(mappedCellValues), rollup.size());
                }
                rollups.add(
                    new RollupInfo(
                        request,
                        rollup));
                rollupBitmaps.add(request.getConstrainedColumnsBitKey());
                converterMap.put(
                    SegmentCacheIndexImpl.makeConverterKey(request),
                    new SegmentBuilder.StarSegmentConverter(
                        measure,
                        key.getCompoundPredicateList()));
                return true;
            }
        }
        return false;
    }

      /**
       * Checks if the request can be satisfied by a rollup already in place
       * and moves that rollup to the top of the list if not there.
       */
      private boolean isRequestCoveredByRollups(CellRequest request) {
          BitKey bitKey = request.getConstrainedColumnsBitKey();
          if (!rollupBitmaps.contains(bitKey)) {
              return false;
          }
          List<SegmentHeader> firstOkList = null;
          for (RollupInfo rollupInfo : rollups) {
              if (!rollupInfo.constrainedColumnsBitKey.equals(bitKey)) {
                  continue;
              }
              int candidateListsIdx = 0;
              // bitkey is the same, are the constrained values compatible?
              candidatesLoop:
                  for (List<SegmentHeader> candList
                      : rollupInfo.candidateLists)
                  {
                      for (SegmentHeader header : candList) {
                          if (headerCoversRequest(header, request)) {
                              firstOkList = candList;
                              break candidatesLoop;
                          }
                      }
                      candidateListsIdx++;
                  }
              if (firstOkList != null) {
                  if (candidateListsIdx > 0) {
                      // move the good candidate list to the front without
                      // discarding the list currently there
                      rollupInfo.candidateLists.remove(candidateListsIdx);
                      rollupInfo.candidateLists.add(0, firstOkList);
                  }
                  return true;
              }
          }
          return false;
      }

      /**
       * Check constraint compatibility
       */
      private boolean headerCoversRequest(
          SegmentHeader header,
          CellRequest request)
      {
          BitKey headerBitKey = header.getConstrainedColumnsBitKey();
          // hard, not assert: a candidate whose bits do not superset the
          // request would let the cursor walk below skip request columns
          // unchecked and claim coverage. Today's callers feed ancestorsOf
          // results (supersets by construction) - this guards the NEXT
          // caller and any future index defect.
          if (!headerBitKey.isSuperSetOf(request.getConstrainedColumnsBitKey())) {
              return false;
          }
          assert request.getConstrainedColumns().length
              == request.getSingleValues().length;
          // two-cursor walk: header bit positions and the request's columns
          // are both bit-ascending, so no per-call index array is needed
          final RolapStar.Column[] requestColumns = request.getConstrainedColumns();
          final Object[] singleValues = request.getSingleValues();
          final List<SegmentColumn> headerColumns =
              header.getConstrainedColumns();
          int i = 0;
          int relevantCCIdx = 0;
          for (int bitPos = headerBitKey.nextSetBit(0); bitPos >= 0;
                  bitPos = headerBitKey.nextSetBit(bitPos + 1)) {
              if (i < requestColumns.length
                  && requestColumns[i].getBitPosition() == bitPos)
              {
                  SortedSet<Comparable> values = headerColumns.get(relevantCCIdx).values;
                  if (values != null && !values.contains(singleValues[i])) {
                      return false;
                  }
                  i++;
              }
              relevantCCIdx++;
          }
          // every request column must have been visited - a leftover means
          // the walk desynchronized and nothing past it was checked
          return i == requestColumns.length;
      }

    private void loadFromSql(
        final CellRequest request,
        final BatchKey key)
    {
        // Finally, add to a batch. It will turn in to a SQL request.
        Batch batch = batches.get(key);
        if (batch == null) {
            batch = new Batch(request);
            batches.put(key, batch);
            if (BitKeyExplain.enabled()) {
                BitKeyExplain.EXPLAIN.debug("new batch {} for measure {}",
                    BitKeyExplain.explain(
                        request.getMeasure().getStar(),
                        request.getConstrainedColumnsBitKey()),
                    request.getMeasure().getName());
            }
            converterMap.put(
                SegmentCacheIndexImpl.makeConverterKey(request),
                new SegmentBuilder.StarSegmentConverter(
                    request.getMeasure(), key.getCompoundPredicateList()));

            if (LOGGER.isDebugEnabled()) {
                StringBuilder buf = new StringBuilder(100);
                buf.append("BatchingCellReader: bitkey=");
                buf.append(request.getConstrainedColumnsBitKey());
                buf.append(Util.NL);

                for (RolapStar.Column column
                    : request.getConstrainedColumns())
                {
                    buf.append("  ");
                    buf.append(column);
                    buf.append(Util.NL);
                }
                LOGGER.debug(buf.toString());
            }
        }
        batch.add(request);
    }

    /**
     * Determines which segments need to be loaded from external cache,
     * created using roll up, or created using SQL to satisfy a given list
     * of cell requests.
     *
     * @return List of segment futures. Each segment future may or may not be
     *    already present (it depends on the current location of the segment
     *    body). Each future will return a not-null segment (or throw).
     */
    LoadBatchResponse load(List<CellRequest> cellRequests) {
        // Check for cancel/timeout. The request might have been on the queue
        // for a while.
        if (executionContext.getExecution() != null) {
            executionContext.getExecution().checkCancelOrTimeout();
        }

        final long t1 = System.currentTimeMillis();

        // Now we're inside the cache manager, we can see which of our cell
        // requests can be answered from cache. Those that can will be added
        // to the segments list; those that can not will be converted into
        // batches and rolled up or loaded using SQL.
        for (CellRequest cellRequest : cellRequests) {
            recordCellRequest2(cellRequest);
        }

        // Sort the batches into deterministic order.
        List<Batch> batchList =
            new ArrayList<>(batches.values());
        Collections.sort(batchList, BatchComparator.instance);
        final List<Future<Map<Segment, SegmentWithData>>> segmentMapFutures =
            new ArrayList<>();
        if (shouldUseGroupingFunction()) {
            LOGGER.debug("Using grouping sets");
            List<CompositeBatch> groupedBatches = groupBatches(batchList);
            for (CompositeBatch batch : groupedBatches) {
                batch.load(segmentMapFutures);
            }
        } else {
            // Load batches in turn.
            for (Batch batch : batchList) {
                batch.loadAggregation(segmentMapFutures);
            }
        }

        if (LOGGER.isDebugEnabled()) {
            final long t2 = System.currentTimeMillis();
            LOGGER.debug("load (millis): {}", (t2 - t1));
        }

        // Create a response and return it to the client. The response is a
        // bunch of work to be done (waiting for segments to load from SQL, to
        // come from cache, and so forth) on the client's time. Some of the bets
        // may not come off, in which case, the client will send us another
        // request.
        final List<SegmentHeader> allCacheHeaders = new ArrayList<>();
        cacheHeaders.values().forEach(allCacheHeaders::addAll);
        return new LoadBatchResponse(
            cacheMgr,
            cellRequests,
            allCacheHeaders,
            rollups,
            converterMap,
            segmentMapFutures,
            futures);
    }

    public static List<CompositeBatch> groupBatches(List<Batch> batchList) {
        Map<BatchKey, CompositeBatch> batchGroups =
            new HashMap<>();
        for (int i = 0; i < batchList.size(); i++) {
            for (int j = i + 1; j < batchList.size();) {
                final Batch iBatch = batchList.get(i);
                final Batch jBatch = batchList.get(j);
                if (iBatch.canBatch(jBatch)) {
                    batchList.remove(j);
                    addToCompositeBatch(batchGroups, iBatch, jBatch);
                } else if (jBatch.canBatch(iBatch)) {
                    batchList.set(i, jBatch);
                    batchList.remove(j);
                    addToCompositeBatch(batchGroups, jBatch, iBatch);
                    j = i + 1;
                } else {
                    j++;
                }
            }
        }

        wrapNonBatchedBatchesWithCompositeBatches(batchList, batchGroups);
        final CompositeBatch[] compositeBatches =
            batchGroups.values().toArray(
                new CompositeBatch[batchGroups.size()]);
        Arrays.sort(compositeBatches, CompositeBatchComparator.instance);
        return Arrays.asList(compositeBatches);
    }

    private static void wrapNonBatchedBatchesWithCompositeBatches(
        List<Batch> batchList,
        Map<BatchKey, CompositeBatch> batchGroups)
    {
        for (Batch batch : batchList) {
            if (batchGroups.get(batch.batchKey) == null) {
                batchGroups.put(batch.batchKey, new CompositeBatch(batch));
            }
        }
    }

    public static void addToCompositeBatch(
        Map<BatchKey, CompositeBatch> batchGroups,
        Batch detailedBatch,
        Batch summaryBatch)
    {
        CompositeBatch compositeBatch = batchGroups.get(detailedBatch.batchKey);

        if (compositeBatch == null) {
            compositeBatch = new CompositeBatch(detailedBatch);
            batchGroups.put(detailedBatch.batchKey, compositeBatch);
        }

        CompositeBatch compositeBatchOfSummaryBatch =
            batchGroups.remove(summaryBatch.batchKey);

        if (compositeBatchOfSummaryBatch != null) {
            compositeBatch.merge(compositeBatchOfSummaryBatch);
        } else {
            compositeBatch.add(summaryBatch);
        }
    }

    /**
     * Command that loads the segments required for a collection of cell
     * requests. Returns the collection of segments.
     */
    public static class LoadBatchCommand implements CacheCommand<LoadBatchResponse>
    {
        private final ExecutionContext executionContext;
        private final SegmentCacheManager cacheMgr;
        private final SqlQueryCapabilities capabilities;
        private final RolapCube cube;
        private final List<CellRequest> cellRequests;

        public LoadBatchCommand(
            ExecutionContext executionContext,
            SegmentCacheManager cacheMgr,
            SqlQueryCapabilities capabilities,
            RolapCube cube,
            List<CellRequest> cellRequests)
        {
            this.executionContext = executionContext;
            this.cacheMgr = cacheMgr;
            this.capabilities = capabilities;
            this.cube = cube;
            this.cellRequests = cellRequests;
        }

        @Override
        public LoadBatchResponse call() {
            return new BatchLoader(executionContext, cacheMgr, capabilities, cube)
                .load(cellRequests);
        }

        @Override
        public ExecutionContext getExecutionContext() {
            return executionContext;
        }
    }

    /**
     * Set of Batches which can grouped together.
     */
    public static class CompositeBatch {
        /** Batch with most number of constraint columns */
        public final Batch detailedBatch;

        /** Batches whose data can be fetched using rollup on detailed batch */
        public final List<Batch> summaryBatches = new ArrayList<>();

        public CompositeBatch(Batch detailedBatch) {
            this.detailedBatch = detailedBatch;
        }

        public void add(Batch summaryBatch) {
            summaryBatches.add(summaryBatch);
        }

        void merge(CompositeBatch summaryBatch) {
            summaryBatches.add(summaryBatch.detailedBatch);
            summaryBatches.addAll(summaryBatch.summaryBatches);
        }

        public void load(
            List<Future<Map<Segment, SegmentWithData>>> segmentFutures)
        {
            GroupingSetsCollector batchCollector =
                new GroupingSetsCollector(true);
            this.detailedBatch.loadAggregation(batchCollector, segmentFutures);

            int cellRequestCount = 0;
            for (Batch batch : summaryBatches) {
                batch.loadAggregation(batchCollector, segmentFutures);
                cellRequestCount += batch.cellRequestCount;
            }

            getSegmentLoader().load(
                cellRequestCount,
                batchCollector.getGroupingSets(),
                detailedBatch.batchKey.getCompoundPredicateList(),
                segmentFutures);
        }

        SegmentLoader getSegmentLoader() {
            return new SegmentLoader(detailedBatch.getSegmentCacheManager());
        }
    }

    private static final Logger BATCH_LOGGER = LoggerFactory.getLogger(BatchLoader.class);

    public static class RollupInfo {
        final RolapStar.Column[] constrainedColumns;
        final BitKey constrainedColumnsBitKey;
        final RolapStar.Measure measure;
        final List<List<SegmentHeader>> candidateLists;

        RollupInfo(
            CellRequest request,
            List<List<SegmentHeader>> candidateLists)
        {
            this.candidateLists = candidateLists;
            constrainedColumns = request.getConstrainedColumns();
            constrainedColumnsBitKey = request.getConstrainedColumnsBitKey();
            measure = request.getMeasure();
        }
    }

    /**
     * Request sent from cache manager to a worker to load segments into
     * the cache, create segments by rolling up, and to wait for segments
     * being loaded via SQL.
     */
    static class LoadBatchResponse {
        private final SegmentCacheManager cacheMgr;
        /**
         * List of segments that are being loaded using SQL.
         *
         * Other workers are executing the SQL. When done, they will write a
         * segment body or an error into the respective futures. The thread
         * processing this request will wait on those futures, once all segments
         * have successfully arrived from cache.
         */
        final List<Future<Map<Segment, SegmentWithData>>> sqlSegmentMapFutures;

        /**
         * List of segments we are trying to load from the cache.
         */
        final List<SegmentHeader> cacheSegments;

        /**
         * List of cell requests that will be satisfied by segments we are
         * trying to load from the cache (or create by rolling up).
         */
        final List<CellRequest> cellRequests;

        /**
         * List of segments to be created from segments in the cache, provided
         * that the cache segments come through.
         *
         * If they do not, we will need to tell the cache manager to remove
         * the pending segments.
         */
        final List<RollupInfo> rollups;

        final Map<SegmentIdentity.FactKey, SegmentBuilder.SegmentConverter> converterMap;

        final Map<SegmentHeader, Future<SegmentBody>> futures;

        LoadBatchResponse(
            SegmentCacheManager cacheMgr,
            List<CellRequest> cellRequests,
            List<SegmentHeader> cacheSegments,
            List<RollupInfo> rollups,
            Map<SegmentIdentity.FactKey, SegmentBuilder.SegmentConverter> converterMap,
            List<Future<Map<Segment, SegmentWithData>>> sqlSegmentMapFutures,
            Map<SegmentHeader, Future<SegmentBody>> futures)
        {
            this.cacheMgr = cacheMgr;
            this.cellRequests = cellRequests;
            this.sqlSegmentMapFutures = sqlSegmentMapFutures;
            this.cacheSegments = cacheSegments;
            this.rollups = rollups;
            this.converterMap = converterMap;
            this.futures = futures;
        }

        public SegmentWithData convert(
            SegmentHeader header,
            SegmentBody body)
        {
            // reconstruction covers every structural predicate shape; the
            // request-scoped map remains for Opaque compound predicates,
            // which only the originating request can rebuild - and for
            // headers whose requesting star is not resolvable yet (null
            // star makes getConverter return null by design)
            SegmentBuilder.SegmentConverter converter =
                cacheMgr.getConverter(
                    requestingStar(header), header);
            if (converter == null) {
                converter = converterMap.get(
                    SegmentCacheIndexImpl.makeConverterKey(header));
            }
            return converter.convert(header, body);
        }

        /**
         * The star of the request that asked for this header. Content-
         * identical catalogs register separate stars over the same segment
         * ids; resolving the star from the header alone picked whichever
         * catalog was cached first - a converter bound to the WRONG star
         * registered the segment into that catalog's working store while
         * the query read its own and re-requested the same cells forever
         * (or NPEd when the first matching catalog had not built the star
         * yet).
         */
        private RolapStar requestingStar(SegmentHeader header) {
            for (CellRequest request : cellRequests) {
                RolapStar star = request.getMeasure().getStar();
                if (star.getFactTable().getAlias()
                        .equals(header.rolapStarFactTableName)
                    && star.getCatalog().getChecksum()
                        .equals(header.schemaChecksum)) {
                    return star;
                }
            }
            return cacheMgr.getStar(header);
        }
    }

    /**
     * Batch grouping key: star, dimensionality and compound predicates.
     * Equality uses the canonical wire form — the same identity the segment
     * index keys on; the runtime predicate list rides along for SQL.
     */
    public static final class BatchKey {
        private final RolapStar star;
        private final BitKey constrainedColumnsBitKey;
        private final List<SegmentPredicate> compoundWire;
        private final List<StarPredicate> compoundPredicateList;
        private final int hash;

        BatchKey(CellRequest request) {
            this.star = request.getMeasure().getStar();
            this.constrainedColumnsBitKey = request.getConstrainedColumnsBitKey();
            this.compoundWire = request.getCompoundPredicates();
            this.compoundPredicateList = request.getCompoundPredicateList();
            this.hash = Objects.hash(star, constrainedColumnsBitKey, compoundWire);
        }

        RolapStar getStar() {
            return star;
        }

        BitKey getConstrainedColumnsBitKey() {
            return constrainedColumnsBitKey;
        }

        List<StarPredicate> getCompoundPredicateList() {
            return compoundPredicateList;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof BatchKey that
                && star == that.star
                && constrainedColumnsBitKey.equals(that.constrainedColumnsBitKey)
                && compoundWire.equals(that.compoundWire);
        }

        @Override
        public String toString() {
            return star.getFactTable().getAlias() + " " + constrainedColumnsBitKey;
        }
    }

    public class Batch {
        // the CellRequest's constrained columns
        final RolapStar.Column[] columns;
        private final Set<RolapStar.Measure> measureSet = new HashSet<>();
        // resolved once per batch; see getAgg
        private AggStar aggStar;
        private final boolean[] aggRollup = {false};
        private boolean aggResolved;
        final List<RolapStar.Measure> measuresList =
            new ArrayList<>();
        final Set<StarColumnPredicate>[] valueSets;
        public final BatchKey batchKey;
        // string representation; for debug; set lazily in toString
        private String string;
        private int cellRequestCount;

        public Batch(CellRequest request) {
            columns = request.getConstrainedColumns();
            valueSets = new HashSet[columns.length];
            for (int i = 0; i < valueSets.length; i++) {
                valueSets[i] = new HashSet<>();
            }
            batchKey = new BatchKey(request);
        }

        @Override
        public String toString() {
            if (string == null) {
                final StringBuilder buf = new StringBuilder();
                buf.append("Batch {\n")
                    .append("  columns={").append(Arrays.toString(columns))
                    .append("}\n")
                    .append("  measures={").append(measuresList).append("}\n")
                    .append("  valueSets={").append(Arrays.toString(valueSets))
                    .append("}\n")
                    .append("  batchKey=").append(batchKey).append("}\n")
                    .append("}");
                string = buf.toString();
            }
            return string;
        }

        public final void add(CellRequest request) {
            ++cellRequestCount;
            final int valueCount = request.getNumValues();
            for (int j = 0; j < valueCount; j++) {
                valueSets[j].add(request.getValueAt(j));
            }
            final RolapStar.Measure measure = request.getMeasure();
            // add() runs once per missed cell: the set carries the contains
            // check, the list keeps the load order
            if (measureSet.add(measure)) {
                assert (measuresList.isEmpty())
                       || (measure.getStar()
                           == (measuresList.getFirst()).getStar())
                    : "Measure must belong to same star as other measures";
                measuresList.add(measure);
            }
        }

        /**
         * Returns the RolapStar associated with the Batch's first Measure.
         *
         * This method can only be called after the {@link #add} method has
         * been called.
         *
         * @return the RolapStar associated with the Batch's first Measure
         */
        private RolapStar getStar() {
            RolapStar.Measure measure = measuresList.getFirst();
            return measure.getStar();
        }

        public BitKey getConstrainedColumnsBitKey() {
            return batchKey.getConstrainedColumnsBitKey();
        }

        public SegmentCacheManager getSegmentCacheManager() {
            return cacheMgr;
        }

        public final void loadAggregation(
            List<Future<Map<Segment, SegmentWithData>>> segmentFutures)
        {
            GroupingSetsCollector collectorWithGroupingSetsTurnedOff =
                new GroupingSetsCollector(false);
            loadAggregation(collectorWithGroupingSetsTurnedOff, segmentFutures);
        }

        public final void loadAggregation(
            GroupingSetsCollector groupingSetsCollector,
            List<Future<Map<Segment, SegmentWithData>>> segmentFutures)
        {
            if (cube.getCatalog().getInternalConnection().getContext().getConfig().generateAggregateSql()) {
                generateAggregateSql();
            }
            boolean optimizePredicates =
                cube.getCatalog().getInternalConnection().getContext().getConfig().optimizePredicates();
            final StarColumnPredicate[] predicates = initPredicates();
            final long t1 = System.currentTimeMillis();

            // TODO: optimize key sets; drop a constraint if more than x% of
            // the members are requested; whether we should get just the cells
            // requested or expand to a n-cube

            // If the database cannot execute "count(distinct ...)", split the
            // distinct aggregations out.
            int distinctMeasureCount = getDistinctMeasureCount(measuresList);
            boolean tooManyDistinctMeasures =
                distinctMeasureCount > 0
                && !capabilities.countDistinct()
                || distinctMeasureCount > 1
                   && !capabilities.multipleCountDistinct()
                || distinctMeasureCount > 0
                   && !capabilities.countDistinctWithOtherAggs();

            if (tooManyDistinctMeasures) {
                doSpecialHandlingOfDistinctCountMeasures(
                    predicates,
                    groupingSetsCollector,
                    segmentFutures, optimizePredicates);
            }

            // Load agg(distinct <SQL expression>) measures individually
            // for DBs that does allow multiple distinct SQL measures.
            if (!capabilities.multipleDistinctSqlMeasures()) {
                // Note that the intention was originally to capture the
                // subquery SQL measures and separate them out; However,
                // without parsing the SQL string, Mondrian cannot distinguish
                // between "col1" + "col2" and subquery. Here the measure list
                // contains both types.

                // See the test case testLoadDistinctSqlMeasure() in
                //  mondrian.rolap.BatchingCellReaderTest

                List<RolapStar.Measure> distinctSqlMeasureList =
                    getDistinctSqlMeasures(measuresList);
                for (RolapStar.Measure measure : distinctSqlMeasureList) {
                    AggregationManager.loadAggregation(
                        cacheMgr,
                        cellRequestCount,
                        Collections.singletonList(measure),
                        columns,
                        batchKey.getStar(),
                        batchKey.getConstrainedColumnsBitKey(),
                        batchKey.getCompoundPredicateList(),
                        predicates,
                        groupingSetsCollector,
                        segmentFutures,
                        optimizePredicates);
                    measuresList.remove(measure);
                }
            }

            final int measureCount = measuresList.size();
            if (measureCount > 0) {
                AggregationManager.loadAggregation(
                    cacheMgr,
                    cellRequestCount,
                    measuresList,
                    columns,
                    batchKey.getStar(),
                    batchKey.getConstrainedColumnsBitKey(),
                    batchKey.getCompoundPredicateList(),
                    predicates,
                    groupingSetsCollector,
                    segmentFutures,
                    optimizePredicates);
            }

            if (BATCH_LOGGER.isDebugEnabled()) {
                final long t2 = System.currentTimeMillis();
                BATCH_LOGGER.debug(
                    "Batch.load (millis) " + (t2 - t1));
            }
        }

        private void doSpecialHandlingOfDistinctCountMeasures(
            StarColumnPredicate[] predicates,
            GroupingSetsCollector groupingSetsCollector,
            List<Future<Map<Segment, SegmentWithData>>> segmentFutures,
            boolean optimizePredicates)
        {
            while (true) {
                // Scan for a measure based upon a distinct aggregation.
                final RolapStar.Measure distinctMeasure =
                    getFirstDistinctMeasure(measuresList);
                if (distinctMeasure == null) {
                    break;
                }
                final String expr =
                    genericSql(distinctMeasure.getExpression());
                final List<RolapStar.Measure> distinctMeasuresList =
                    new ArrayList<>();
                for (int i = 0; i < measuresList.size();) {
                    final RolapStar.Measure measure = measuresList.get(i);
                    if (measure.getAggregator().isDistinct()
                        && genericSql(measure.getExpression())
                        .equals(expr))
                    {
                        measuresList.remove(i);
                        distinctMeasuresList.add(distinctMeasure);
                    } else {
                        i++;
                    }
                }

                // Load all the distinct measures based on the same expression
                // together
                AggregationManager.loadAggregation(
                    cacheMgr,
                    cellRequestCount,
                    distinctMeasuresList,
                    columns,
                    batchKey.getStar(),
                    batchKey.getConstrainedColumnsBitKey(),
                    batchKey.getCompoundPredicateList(),
                    predicates,
                    groupingSetsCollector,
                    segmentFutures,
                    optimizePredicates);
            }
        }

        private StarColumnPredicate[] initPredicates() {
            StarColumnPredicate[] predicates =
                new StarColumnPredicate[columns.length];
            for (int j = 0; j < columns.length; j++) {
                Set<StarColumnPredicate> valueSet = valueSets[j];

                StarColumnPredicate predicate;
                // valueSets entries are always initialized in the Batch
                // constructor - a null here would be a construction bug
                if (valueSet == null) {
                    throw new IllegalStateException(
                        "uninitialized value set for batch column");
                } else {
                    ValueColumnPredicate[] values =
                        valueSet.toArray(
                            new ValueColumnPredicate[valueSet.size()]);
                    // Sort array to achieve determinism in generated SQL.
                    Arrays.sort(
                        values,
                        ValueColumnConstraintComparator.instance);

                    predicate =
                        new ListColumnPredicate(
                            columns[j],
                            Arrays.asList((StarColumnPredicate[]) values));
                }

                predicates[j] = predicate;
            }
            return predicates;
        }

        private void generateAggregateSql() {
            if (cube == null || cube instanceof RolapVirtualCube) {
                final StringBuilder buf = new StringBuilder(64);
                buf.append(
                    "AggGen: Sorry, can not create SQL for virtual Cube \"")
                    .append(cube == null ? null : cube.getName())
                    .append("\", operation not currently supported");
                String msg = buf.toString();
                BATCH_LOGGER.error(msg);

            } else {
                final AggGen aggGen =
                    new AggGen(cube.getName(), cube.getStar(), columns);
                if (aggGen.isReady()) {
                    // PRINT TO STDOUT - DO NOT USE BATCH_LOGGER
                    LOGGER.debug(
                        "createLost:{}{}", Util.NL, aggGen.createLost());
                    LOGGER.debug(
                        "insertIntoLost:{}{}", Util.NL, aggGen.insertIntoLost());
                    LOGGER.debug(
                        "createCollapsed:{}{}", Util.NL, aggGen.createCollapsed());
                    LOGGER.debug("insertIntoCollapsed: {}{}", Util.NL, aggGen.insertIntoCollapsed());
                } else {
                    BATCH_LOGGER.error("AggGen failed");
                }
            }
        }

        /**
         * Returns the first measure based upon a distinct aggregation, or null
         * if there is none.
         */
        final RolapStar.Measure getFirstDistinctMeasure(
            List<RolapStar.Measure> measuresList)
        {
            for (RolapStar.Measure measure : measuresList) {
                if (measure.getAggregator().isDistinct()) {
                    return measure;
                }
            }
            return null;
        }

        /**
         * Returns the number of the measures based upon a distinct
         * aggregation.
         */
        private int getDistinctMeasureCount(
            List<RolapStar.Measure> measuresList)
        {
            int count = 0;
            for (RolapStar.Measure measure : measuresList) {
                if (measure.getAggregator().isDistinct()) {
                    ++count;
                }
            }
            return count;
        }

        /**
         * Returns the list of measures based upon a distinct aggregation
         * containing SQL measure expressions(as opposed to column expressions).
         *
         * This method was initially intended for only those measures that are
         * defined using subqueries(for DBs that support them). However, since
         * Mondrian does not parse the SQL string, the method will count both
         * queries as well as some non query SQL expressions.
         */
        private List<RolapStar.Measure> getDistinctSqlMeasures(
            List<RolapStar.Measure> measuresList)
        {
            List<RolapStar.Measure> distinctSqlMeasureList =
                new ArrayList<>();
            for (RolapStar.Measure measure : measuresList) {
                if (measure.getAggregator().isDistinct()
                    && measure.getExpression() instanceof
                    RolapSqlExpression measureExpr)
                {
                    org.eclipse.daanse.olap.api.SqlStatement measureSql = measureExpr.getSqls().getFirst();
                    // Checks if the SQL contains "SELECT" to detect the case a
                    // subquery is used to define the measure. This is not a
                    // perfect check, because a SQL expression on column names
                    // containing "SELECT" will also be detected. e,g,
                    // count("select beef" + "regular beef").
                    if (measureSql.getSql().toUpperCase().contains("SELECT")) {
                        distinctSqlMeasureList.add(measure);
                    }
                }
            }
            return distinctSqlMeasureList;
        }

        /**
         * Returns whether another Batch can be batched to this Batch.
         *
         * This is possible if:
         * columns list is super set of other batch's constraint columns;
         *     and
         * both have same Fact Table; and
         * matching columns of this and other batch has the same value; and
         * non matching columns of this batch have ALL VALUES
         *
         */
        public boolean canBatch(Batch other) {
            // cheap predicates first: haveSameValues is O(columns x sets)
            // and runs inside the O(n^2) grouping pass
            return !hasDistinctCountMeasure()
                && !other.hasDistinctCountMeasure()
                && hasSameCompoundPredicates(other)
                && hasOverlappingBitKeys(other)
                && hasSameMeasureList(other)
                && haveSameClosureColumns(other)
                && haveSameStarAndAggregation(other)
                && haveSameValues(other);
        }

        /**
         * The composite batch runs ONE SQL with the detailed batch's
         * compound predicate list, while every summary batch registers
         * segment headers claiming its OWN compounds - merging across
         * differing compounds would publish headers whose data was
         * filtered by another batch's WHERE clause.
         */
        private boolean hasSameCompoundPredicates(Batch other) {
            return batchKey.compoundWire.equals(other.batchKey.compoundWire);
        }

        private boolean hasSameMeasureList(Batch other) {
            return this.measureSet.equals(other.measureSet);
        }

        boolean hasOverlappingBitKeys(Batch other) {
            return getConstrainedColumnsBitKey()
                .isSuperSetOf(other.getConstrainedColumnsBitKey());
        }

        boolean hasDistinctCountMeasure() {
            return getDistinctMeasureCount(measuresList) > 0;
        }

        boolean haveSameStarAndAggregation(Batch other) {
            if (!getStar().equals(other.getStar())) {
                return false;
            }
            boolean[] rollup = {false};
            boolean[] otherRollup = {false};
            return getAgg(rollup) == other.getAgg(otherRollup)
                && rollup[0] == otherRollup[0];
        }

        /**
         * Returns whether this batch has the same closure columns as another.
         *
         * Ensures that we do not group together a batch that includes a
         * level of a parent-child closure dimension with a batch that does not.
         * It is not safe to roll up from a parent-child closure level; due to
         * multiple accounting, the 'all' level is less than the sum of the
         * members of the closure level.
         *
         * @param other Other batch
         * @return Whether batches have the same closure columns
         */
        boolean haveSameClosureColumns(Batch other) {
            final BitKey cubeClosureColumnBitKey = cube.getClosureColumnBitKey();
            if (cubeClosureColumnBitKey == null) {
                // Virtual cubes have a null bitkey. For now, punt; should do
                // better.
                return true;
            }
            final BitKey closureColumns =
                this.batchKey.getConstrainedColumnsBitKey()
                    .and(cubeClosureColumnBitKey);
            final BitKey otherClosureColumns =
                other.batchKey.getConstrainedColumnsBitKey()
                    .and(cubeClosureColumnBitKey);
            return closureColumns.equals(otherClosureColumns);
        } 

        /**
         * Memoized per batch: the O(n²) batch grouping asks every pair, and
         * findAgg walks all AggStars each time. The batch's columns and
         * measures are complete before grouping starts.
         *
         * @param rollup Out parameter
         * @return AggStar
         */
        private AggStar getAgg(boolean[] rollup) {
            if (!aggResolved) {
                aggStar = AggregationManager.findAgg(
                    getStar(),
                    getConstrainedColumnsBitKey(),
                    makeMeasureBitKey(),
                    aggRollup);
                aggResolved = true;
            }
            rollup[0] = aggRollup[0];
            return aggStar;
        }

        private BitKey makeMeasureBitKey() {
            BitKey bitKey = getConstrainedColumnsBitKey().emptyCopy();
            for (RolapStar.Measure measure : measuresList) {
                bitKey.set(measure.getBitPosition());
            }
            return bitKey;
        }

        /**
         * Return whether have same values for overlapping columns or
         * has all children for others.
         */
        boolean haveSameValues(
            Batch other)
        {
            for (int j = 0; j < columns.length; j++) {
                boolean isCommonColumn = false;
                for (int i = 0; i < other.columns.length; i++) {
                    if (areSameColumns(other.columns[i], columns[j])) {
                        if (hasSameValues(other.valueSets[i], valueSets[j])) {
                            isCommonColumn = true;
                            break;
                        } else {
                            return false;
                        }
                    }
                }
                if (!isCommonColumn
                    && !hasAllValues(columns[j], valueSets[j]))
                {
                    return false;
                }
            }
            return true;
        }

        private boolean hasAllValues(
            RolapStar.Column column,
            Set<StarColumnPredicate> valueSet)
        {
            return column.getCardinality() == valueSet.size();
        }

        private boolean areSameColumns(
            RolapStar.Column otherColumn,
            RolapStar.Column thisColumn)
        {
            return otherColumn.equals(thisColumn);
        }

        private boolean hasSameValues(
            Set<StarColumnPredicate> otherValueSet,
            Set<StarColumnPredicate> thisValueSet)
        {
            return otherValueSet.equals(thisValueSet);
        }
    }

    public static class CompositeBatchComparator
        implements Comparator<CompositeBatch>
    {
        static final CompositeBatchComparator instance =
            new CompositeBatchComparator();

        @Override
        public int compare(CompositeBatch o1, CompositeBatch o2) {
            return BatchComparator.instance.compare(
                o1.detailedBatch,
                o2.detailedBatch);
        }
    }

    public static class BatchComparator implements Comparator<Batch> {
        static final BatchComparator instance = new BatchComparator();

        private BatchComparator() {
        }

        @Override
        public int compare(
            Batch o1, Batch o2)
        {
            if (o1.columns.length != o2.columns.length) {
                return o1.columns.length - o2.columns.length;
            }
            for (int i = 0; i < o1.columns.length; i++) {
                int c = o1.columns[i].getName().compareTo(
                    o2.columns[i].getName());
                if (c != 0) {
                    return c;
                }
            }
            for (int i = 0; i < o1.columns.length; i++) {
                int c = compare(o1.valueSets[i], o2.valueSets[i]);
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }

        <T> int compare(Set<T> set1, Set<T> set2) {
            if (set1.size() != set2.size()) {
                return set1.size() - set2.size();
            }
            // HashSet iteration order is arbitrary: compare order-insensitive
            // sorted views, or the comparator loses transitivity and sort()
            // throws
            List<T> list1 = new ArrayList<>(set1);
            List<T> list2 = new ArrayList<>(set2);
            Comparator<T> byKey = Util::compareKey;
            list1.sort(byKey);
            list2.sort(byKey);
            for (int i = 0; i < list1.size(); i++) {
                int c = Util.compareKey(list1.get(i), list2.get(i));
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }
    }

    public static class ValueColumnConstraintComparator
        implements Comparator<ValueColumnPredicate>
    {
        static final ValueColumnConstraintComparator instance =
            new ValueColumnConstraintComparator();

        private ValueColumnConstraintComparator() {
        }

        @Override
        public int compare(
            ValueColumnPredicate o1,
            ValueColumnPredicate o2)
        {
            Object v1 = o1.getValue();
            Object v2 = o2.getValue();
            if (v1.getClass() == v2.getClass()
                && v1 instanceof Comparable comparable)
            {
                return comparable.compareTo(v2);
            } else {
                return v1.toString().compareTo(v2.toString());
            }
        }
    }

}
