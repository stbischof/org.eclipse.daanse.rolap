/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (c) 2002-2020 Hitachi Vantara.
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
package org.eclipse.daanse.rolap.common.agg;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.eclipse.daanse.olap.api.Context;
import org.eclipse.daanse.olap.api.cache.CacheCommand;
import org.eclipse.daanse.olap.api.cache.CacheControl.CellRegion;
import org.eclipse.daanse.olap.api.cache.OlapSegmentCacheIndexRegistry;
import org.eclipse.daanse.olap.api.cache.OlapSegmentCacheManager;
import org.eclipse.daanse.olap.api.element.Member;
import org.eclipse.daanse.olap.api.exception.OlapRuntimeException;
import org.eclipse.daanse.olap.api.execution.Execution;
import org.eclipse.daanse.olap.api.execution.ExecutionContext;
import org.eclipse.daanse.olap.api.monitor.EventBus;
import org.eclipse.daanse.olap.api.monitor.event.CellCacheEvent;
import org.eclipse.daanse.olap.api.monitor.event.CellCacheEventCommon;
import org.eclipse.daanse.olap.api.monitor.event.CellCacheSegmentCreateEvent;
import org.eclipse.daanse.olap.api.monitor.event.CellCacheSegmentDeleteEvent;
import org.eclipse.daanse.olap.api.monitor.event.ConnectionEventCommon;
import org.eclipse.daanse.olap.api.monitor.event.EventCommon;
import org.eclipse.daanse.olap.api.monitor.event.ExecutionEventCommon;
import org.eclipse.daanse.olap.api.monitor.event.MdxStatementEventCommon;
import org.eclipse.daanse.olap.api.monitor.event.ServertEventCommon;
import org.eclipse.daanse.olap.common.Util;
import org.eclipse.daanse.olap.spi.SegmentBody;
import org.eclipse.daanse.olap.spi.SegmentCache;
import org.eclipse.daanse.olap.spi.SegmentColumn;
import org.eclipse.daanse.olap.spi.SegmentHeader;
import org.eclipse.daanse.olap.util.ByteString;
import org.eclipse.daanse.olap.spi.SegmentIdentity;
import org.eclipse.daanse.olap.util.Pair;
import org.eclipse.daanse.rolap.api.RolapContext;
import org.eclipse.daanse.rolap.common.CacheControlImpl;
import org.eclipse.daanse.rolap.common.RolapUtil;
import org.eclipse.daanse.rolap.common.cache.MemorySegmentCache;
import org.eclipse.daanse.rolap.common.cache.SegmentCacheIndex;
import org.eclipse.daanse.rolap.common.cache.SegmentCacheIndexImpl;
import org.eclipse.daanse.rolap.common.catalog.RolapCatalogKey;
import org.eclipse.daanse.rolap.common.star.BitKeyExplain;
import org.eclipse.daanse.rolap.common.star.RolapStar;
import org.eclipse.daanse.rolap.element.RolapCatalog;
import org.eclipse.daanse.rolap.element.RolapStoredMeasure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Active object around the segment caches. One dedicated actor thread
 * applies all index mutations: commands run synchronously via
 * {@code execute}, events asynchronously via {@code event}. The
 * {@code cacheExecutor} carries external cache traffic, the
 * {@code sqlExecutor} the segment SQL. The composite cache chains the
 * local memory store with every attached external cache: first hit wins
 * on get, puts and removes go to all. Writes of the same segment are
 * sequenced per unique id ({@link #sequencedCacheOp}), so a flush issued
 * after a put can never be overtaken by it.
 *
 * Two further roles: session OVERLAYS ({@link #sessionOverlay}) are
 * thread-free twins sharing this manager's actor and pools but owning a
 * private index registry and in-memory store - writeback isolation
 * lives there; and PRIMING - attaching an external store schedules a
 * keys-only star inventory and loads headers per star lazily
 * ({@code schedulePrimingIfPending}-style markers in
 * {@code factTablesToPrime}) instead of pulling the full listing over
 * the wire.
 */
@SuppressWarnings( { "JavaDoc", "squid:S1192" } )
// suppressing warnings for duplicated string constants (mostly used in
// trace logging) and javadoc field accessibility
public class SegmentCacheManager implements OlapSegmentCacheManager {
  private final Actor actor;
  final Thread thread;
  private final boolean overlay;
  private final AtomicBoolean shutdownStarted = new AtomicBoolean();
  // orders external put/remove per segment header id (not per catalog) on the
  // cache executor: a flush issued after a put can never be overtaken by it.
  // Entries remove themselves when their chain completes
  private final SequencedStoreOps storeOps;

  /**
   * In-flight priming listings (attach-time and marker-triggered): store
   * I/O dispatched to the cache executor. Drained by
   * {@link #awaitPendingCacheWrites()} so warm-start probes and shutdown
   * see a quiesced priming pipeline, not a race.
   */
  private final java.util.Queue<java.util.concurrent.Future<?>> pendingPrimingListings =
      new java.util.concurrent.ConcurrentLinkedQueue<>();
  private final Map<SegmentCache, SegmentCache.SegmentCacheListener> externalCacheListeners =
      new ConcurrentHashMap<>();
  // keyed by (checksum, fact alias): an alias-only marker let catalog A
  // consume catalog B's pending prime (same alias, different checksum)
  private final Set<SegmentCache.StarKey> factTablesToPrime;

  /**
   * Converters per (star, fact key), positive results only. Weak star
   * keys: an evicted catalog releases its stars and their memo with them.
   */
  private final Map<RolapStar, Map<SegmentIdentity.FactKey, SegmentBuilder.SegmentConverter>> converterMemo =
      Collections.synchronizedMap( new WeakHashMap<>() );

  /**
   * Executor with which to send requests to external caches.
   */
  final ExecutorService cacheExecutor;
  // Statement cancels are JDBC network round-trips (PG/MySQL open a new
  // connection per cancel) with the opposite latency profile of store
  // ops - a cancel storm against a dead database must not starve the
  // pool that carries every store put/remove. Cached: cancels are short
  // and bursty, idle threads die away.
  private final ExecutorService statementCancelExecutor;

  /**
   * Executor with which to execute SQL requests.
   *
   * TODO: create using factory and/or configuration parameters; could be
   * shared per context or per target JDBC database.
   */
  final ExecutorService sqlExecutor;

  // mutated at runtime: whiteboard attach/detach adds and removes workers
  public final List<SegmentCacheWorker> segmentCacheWorkers =
    new CopyOnWriteArrayList<>();

  public final SegmentCache compositeCache;
  private final SegmentCacheIndexRegistry indexRegistry;

  private static final Logger LOGGER =
    LoggerFactory.getLogger( SegmentCacheManager.class );
  private final RolapContext context;
    // NOTE: both limit messages are currently UNREACHABLE - the pools run
    // on an unbounded LinkedBlockingQueue, so their rejection handlers fire
    // only after shutdown, and that case is answered by the isShutdown()
    // branch first. maxSqlThreads/maxCacheThreads size the pools, they do
    // not cause rejections.
    private static final String SQL_QUERY_LIMIT_REACHED = """
    The number of concurrent SQL statements which can be used simultaneously by this Daanse server instance has been reached. Set ''daanse.rolap.maxSqlThreads'' to change the current limit.
    """;
    private static final String SEGMENT_CACHE_LIMIT_REACHED = """
    The number of concurrent segment cache operations which can be run simultaneously by this Daanse server instance has been reached. Set ''daanse.rolap.maxCacheThreads'' to change the current limit.
    """;

    public SegmentCacheManager( RolapContext context ) {
    this.context = context;
    this.overlay = false;
    this.sqlExecutor = createSqlExecutor(context);
    this.cacheExecutor = createCacheExecutor(context);
    this.storeOps = new SequencedStoreOps( this.cacheExecutor );
    this.statementCancelExecutor = Executors.newCachedThreadPool( runnable -> {
      Thread cancelThread =
          new Thread( runnable, "daanse.rolap.agg.SegmentCacheManager$statementCancelExecutor" );
      cancelThread.setDaemon( true );
      return cancelThread;
    } );
    actor = new Actor();
    thread = actor.thread;

    // Create the index registry.
    this.indexRegistry = new SegmentCacheIndexRegistry();

    // Add a local cache, if needed.
    if ( !context.getConfig().disableLocalSegmentCache()
      && !context.getConfig().disableCaching() ) {
      final MemorySegmentCache cache = new MemorySegmentCache();
      segmentCacheWorkers.add(
        new SegmentCacheWorker( cache, thread ) );
    }

    compositeCache = new CompositeSegmentCache( segmentCacheWorkers, context.getConfig().disableCaching() );
    factTablesToPrime = ConcurrentHashMap.newKeySet();
  }

  /**
   * Attaches an external segment cache: worker, index listener and priming
   * of already loaded stars. The provider owns the cache lifecycle.
   */
  public void addExternalCache( SegmentCache cache ) {
    if ( overlay || context.getConfig().disableCaching() ) {
      return;
    }
    // claim the listener slot FIRST: two concurrent binds of the same
    // cache otherwise both pass a containsKey check and register two
    // workers (double puts, double stats) plus an unremovable listener
    AsyncCacheListener listener = new AsyncCacheListener( this, context );
    if ( externalCacheListeners.putIfAbsent( cache, listener ) != null ) {
      return;
    }
    try {
      segmentCacheWorkers.add( new SegmentCacheWorker( cache, thread ) );
      cache.addListener( listener );
    } catch ( RuntimeException | Error e ) {
      // roll back the half-attached state: a thrown addListener or listing
      // otherwise left a worker without invalidations AND blocked every
      // retry behind the stale listener-map entry
      externalCacheListeners.remove( cache, listener );
      segmentCacheWorkers.removeIf( worker -> {
        if ( worker.cache == cache ) {
          worker.markClosing();
          return true;
        }
        return false;
      } );
      try {
        cache.removeListener( listener );
      } catch ( RuntimeException cleanup ) {
        LOGGER.debug( "listener cleanup after failed attach", cleanup );
      }
      throw e;
    }
    // the inventory listing is store I/O and runs OFF the bind thread
    // (which holds the context's segment-cache monitor) on the cache
    // executor, like schedulePrimingIfPending. Failure degrades instead
    // of unwinding the attach: priming is eventually consistent, a query
    // in the window loads cold and the markers retry on the next attach.
    try {
      pendingPrimingListings.removeIf( java.util.concurrent.Future::isDone );
      pendingPrimingListings.add( cacheExecutor.submit( () -> {
        try {
          // the star inventory only: keys-side on the store, never the
          // full header listing over the wire
          final Set<SegmentCache.StarKey> knownStars = cache.knownStars();
          factTablesToPrime.addAll( knownStars );
          for ( Object catalog : context.getCatalogCache().getCachedCatalogs() ) {
            var rolapCatalog = (RolapCatalog) catalog;
            for ( RolapStar star : rolapCatalog.getRolapStarRegistry().getStars() ) {
              primeStarFrom( cache, star, knownStars );
            }
          }
        } catch ( RuntimeException | Error e ) {
          LOGGER.warn( "priming from attached cache failed; queries load cold", e );
        }
      } ) );
    } catch ( RuntimeException rejected ) {
      LOGGER.debug( "attach-time priming rejected", rejected );
    }
  }

  /** Primes one loaded star from the newly attached cache's inventory. */
  private void primeStarFrom(
      SegmentCache cache, RolapStar star, Set<SegmentCache.StarKey> knownStars ) {
    final SegmentCache.StarKey starKey = new SegmentCache.StarKey(
        star.getCatalog().getChecksum().toString(),
        star.getFactTable().getAlias() );
    if ( !knownStars.contains( starKey ) ) {
      return;
    }
    // per-star prefix match: only this star's headers travel
    final List<SegmentHeader> matching = cache.getSegmentHeaders(
        star.getCatalog().getChecksum(), star.getFactTable().getAlias() );
    if ( !matching.isEmpty() ) {
      actor.event(
        this,
        new PrimeStarEvent( EventContext.external( context ), star, matching ) );
    }
    // consume the marker: this star is primed - leaving it made the next
    // schedulePrimingIfPending list the store a second time
    factTablesToPrime.remove( starKey );
  }

  /** Detaches the cache without tearing it down; the provider owns it. */
  public void removeExternalCache( SegmentCache cache ) {
    SegmentCache.SegmentCacheListener listener = externalCacheListeners.remove( cache );
    if ( listener != null ) {
      cache.removeListener( listener );
    }
    segmentCacheWorkers.removeIf( worker -> {
      if ( worker.cache == cache ) {
        // in-flight executor calls degrade to a miss; stale index headers
        // heal on the next lookup (miss removes them and the load retries)
        worker.markClosing();
        return true;
      }
      return false;
    } );
  }


  /**
   * A session overlay isolates one session's segments (writeback pending
   * rows) without starting threads: it shares the actor, its thread and both
   * executors with the given manager and owns only its index registry plus a
   * local in-memory store. Session values never reach an external cache.
   */
  public static SegmentCacheManager sessionOverlay( SegmentCacheManager shared ) {
    return new SegmentCacheManager( shared );
  }

  private SegmentCacheManager( SegmentCacheManager shared ) {
    this.context = shared.context;
    this.overlay = true;
    this.sqlExecutor = shared.sqlExecutor;
    this.cacheExecutor = shared.cacheExecutor;
    this.storeOps = new SequencedStoreOps( this.cacheExecutor );
    this.statementCancelExecutor = shared.statementCancelExecutor;
    this.actor = shared.actor;
    this.thread = shared.thread;
    this.indexRegistry = new SegmentCacheIndexRegistry();
    if ( !context.getConfig().disableCaching() ) {
      segmentCacheWorkers.add( new SegmentCacheWorker( new MemorySegmentCache(), thread ) );
    }
    compositeCache = new CompositeSegmentCache( segmentCacheWorkers, context.getConfig().disableCaching() );
    factTablesToPrime = ConcurrentHashMap.newKeySet();
  }

    private ExecutorService createCacheExecutor(Context<?> context) {
        return Util.getExecutorService(
            // We use the same value for coreSize and maxSize
            // because that's the behavior we want. All extra
            // tasks will be put on an unbounded queue.
            context.getConfig().segmentCacheManagerNumberCacheThreads(),
            context.getConfig().segmentCacheManagerNumberCacheThreads(),
            60,
            "daanse.rolap.agg.SegmentCacheManager$cacheExecutor",
            ( r, executor ) -> {
                if ( executor.isShutdown() ) {
                    throw new OlapRuntimeException( "Cache operation submitted after shutdown" );
                }
                throw new OlapRuntimeException(SEGMENT_CACHE_LIMIT_REACHED);
            } );
    }

    private ExecutorService createSqlExecutor(Context<?> context) {
        return Util.getExecutorService(
            // We use the same value for coreSize and maxSize
            // because that's the behavior we want. All extra
            // tasks will be put on an unbounded queue.
            context.getConfig().segmentCacheManagerNumberSqlThreads(),
            context.getConfig().segmentCacheManagerNumberSqlThreads(),
            60,
            "daanse.rolap.agg.SegmentCacheManager$sqlExecutor",
            ( r, executor ) -> {
                if ( executor.isShutdown() ) {
                    throw new OlapRuntimeException( "Segment SQL submitted after shutdown" );
                }
                throw new OlapRuntimeException(SQL_QUERY_LIMIT_REACHED);
            } );
    }

  /** Live counters per attached cache, in composite order. */
  public List<SegmentCacheStats> getCacheStats() {
    List<SegmentCacheStats> stats = new ArrayList<>( segmentCacheWorkers.size() );
    for ( SegmentCacheWorker worker : segmentCacheWorkers ) {
      stats.add( worker.stats );
    }
    return stats;
  }

  /**
   * Runs a cache operation on the cache executor, ordered with every other
   * sequenced operation on the same segment id. The chain entry cleans
   * itself up once it drains.
   */
  public <T> CompletableFuture<T> sequencedCacheOp( SegmentHeader header, Supplier<T> op ) {
    return storeOps.sequenced( header.getUniqueID(), op );
  }

  /**
   * Sequences an operation under BOTH segment ids: a rename reads under
   * the old id and writes under the new, so follow-ups on either id must
   * run after it. See {@link SequencedStoreOps} for the ordering rules.
   */
  public <T> CompletableFuture<T> sequencedCacheOp(
      SegmentHeader oldHeader, SegmentHeader newHeader, Supplier<T> op ) {
    return storeOps.sequenced( oldHeader.getUniqueID(), newHeader.getUniqueID(), op );
  }

  /**
   * Consumes the star's priming marker and ENQUEUES the header listing -
   * a true return is a promise, not completion: the headers arrive on
   * the cache executor and land in the index via the actor later.
   */
  public boolean schedulePrimingIfPending( RolapStar star ) {
    final String alias = star.getFactTable().getAlias();
    final SegmentCache.StarKey starKey = new SegmentCache.StarKey(
        star.getCatalog().getChecksum().toString(), alias );
    if ( !factTablesToPrime.remove( starKey ) ) {
      return false;
    }
    // the store listing is network I/O and runs on the cache executor:
    // callers hold catalog-build locks the actor also takes, and the
    // flush path reaches here ON the actor (writeback fact mismatch) -
    // synchronous listing put store latency under both. Priming is
    // eventually consistent; a query in the window loads cold.
    try {
      pendingPrimingListings.removeIf( java.util.concurrent.Future::isDone );
      pendingPrimingListings.add( cacheExecutor.submit( () -> {
        try {
          // per-star prefix match — the full inventory never travels here
          final List<SegmentHeader> matching = compositeCache.getSegmentHeaders(
              star.getCatalog().getChecksum(), alias );
          if ( !matching.isEmpty() ) {
            actor.event(
              this,
              new PrimeStarEvent( EventContext.external( context ), star, matching ) );
          }
        } catch ( RuntimeException | Error e ) {
          factTablesToPrime.add( starKey );
          LOGGER.warn( "priming star " + alias + " failed; marker restored", e );
        }
      } ) );
    } catch ( RuntimeException e ) {
      // rejected submit (the pool's handler reports shutdown as
      // OlapRuntimeException): keep the marker for a later attach
      factTablesToPrime.add( starKey );
    }
    return true;
  }

  @Override
  public <T> T execute( CacheCommand<T> command ) {
    return actor.execute( command );
  }

  public OlapSegmentCacheIndexRegistry getIndexRegistry() {
    return indexRegistry;
  }

  /**
   * Adds a segment to segment index.
   *
   * Called when a SQL statement has finished loading a segment.
   *
   * The handling event fills the waiting slot AND decides the store put:
   * only a header the index still holds is enqueued for the external
   * caches (sequenced per segment id, off the actor) - a segment a flush
   * removed mid-load gets no put at all.
   *
   * @param header segment header
   * @param body   segment body
   */
  void loadSucceeded(
    RolapStar star,
    SegmentHeader header,
    SegmentBody body,
    CellCacheEvent.Source source ) {
    actor.event(
      this,
      new SegmentLoadSucceededEvent( EventContext.current(), star, header, body, source ) );
  }

  /** Whether segments of the cube go to the caches at all. */
  public boolean isSegmentCachingEnabled( RolapStar star, String cubeName ) {
    return !context.getConfig().disableCaching()
        && star.getCatalog().isCellCachingEnabled( cubeName );
  }

  /**
   * Publishes a finished segment: releases every query waiting on the
   * slot (with the monitor events). The store put is decided ON THE
   * ACTOR inside the load-succeeded event and only happens while the
   * index still holds the header - a flush that removed it wins; the
   * sequencing per segment id merely orders the store ops that do get
   * enqueued. Shared by the SQL load and the rollup path.
   */
  public void cacheLoaded( RolapStar star, SegmentHeader header, SegmentBody body,
      CellCacheEvent.Source source ) {
    if ( !isSegmentCachingEnabled( star, header.cubeName ) ) {
      return;
    }
    if ( BitKeyExplain.enabled() ) {
      BitKeyExplain.EXPLAIN.debug( "publish segment {} | dimensionality {}",
          BitKeyExplain.explain( header ),
          BitKeyExplain.explain( star, header.constrainedColsBitKey ) );
    }
    // the store put is decided ON THE ACTOR, inside the load-succeeded
    // event: only a header the index still holds may reach the stores.
    // The unconditional caller-side put wrote the pre-flush body of a
    // segment a flush had just constrained away back into every store
    // under the OLD id (and Redis published its birth cluster-wide) -
    // the flush was silently non-persistent.
    loadSucceeded( star, header, body, source );
  }

  /**
   * Informs cache manager that a segment load failed.
   *
   * Called when a SQL statement receives an error while loading a
   * segment.
   *
   * @param header    segment header
   * @param throwable Error
   */
  public void loadFailed(
    RolapStar star,
    SegmentHeader header,
    Throwable throwable ) {
    actor.event(
      this,
      new SegmentLoadFailedEvent( star, header, throwable ) );
  }

  /**
   * Removes a segment from segment index.
   *
   * Call is asynchronous. It comes back immediately.
   *
   * Does not remove it from the external cache.
   *
   * @param header segment header
   */
  public void remove(
    RolapStar star,
    SegmentHeader header ) {
    actor.event(
      this,
      new SegmentRemoveEvent( EventContext.current(), star, header ) );
  }

  /**
   * Tells the cache that a segment is newly available in an external cache.
   */
  public void externalSegmentCreated(
    SegmentHeader header,
    Context<?> context ) {
    if ( context.getConfig().disableCaching() ) {
      // Ignore cache requests.
      return;
    }
    actor.event(
      this,
      new ExternalSegmentCreatedEvent( EventContext.external( context ), header ) );
  }

  /**
   * Tells the cache that a segment is no longer available in an external cache.
   */
  public void externalSegmentDeleted(
    SegmentHeader header,
    Context<?> context ) {
    if ( context.getConfig().disableCaching() ) {
      // Ignore cache requests.
      return;
    }
    actor.event(
      this,
      new ExternalSegmentDeletedEvent( EventContext.external( context ), header ) );
  }

  @Override
  public void printCacheState(
    CellRegion region,
    PrintWriter pw,
    ExecutionContext executionContext ) {
    // render on the actor, write on the caller: the caller's writer may
    // be network- or file-bound, and the actor never waits on foreign I/O
    pw.print( actor.execute(
      new PrintCacheStateCommand( region, executionContext ) ) );
    pw.flush();
  }

  /**
   * Shuts down this cache manager and all active threads and indexes. An
   * overlay only clears its local store; actor and executors belong to the
   * shared manager.
   */
  /**
   * The context-free subset of {@link #shutdown()} for orphaned contexts:
   * captures ONLY the four executors - never {@code this}, and never the
   * workers or stores (the in-memory store's listener chain reaches the
   * manager and through it the context; capturing it would pin the very
   * graph this cleanup exists to release). The store heap needs no
   * explicit teardown here: it is ordinary garbage once the context is
   * unreachable - only the pool THREADS are GC roots that must be told
   * to stop.
   */
  Runnable orphanCleanup() {
    final ExecutorService actorExecutor = actor.executor;
    final ExecutorService cachePool = cacheExecutor;
    final ExecutorService sqlPool = sqlExecutor;
    final ExecutorService cancelPool = statementCancelExecutor;
    return () -> {
      // orphaned: nothing queued matters any more - drop, don't drain
      actorExecutor.shutdownNow();
      cachePool.shutdownNow();
      sqlPool.shutdownNow();
      cancelPool.shutdownNow();
    };
  }

  @Override
  public void shutdown() {
    if ( overlay ) {
      // shut the private store down but keep the LIST: the composite holds
      // it by reference, and a statement still running on this session
      // (two statements per XMLA connection are normal) must degrade to
      // clean misses on closed workers - not find a structurally emptied
      // composite whose puts vanish without a trace
      for ( SegmentCacheWorker worker : segmentCacheWorkers ) {
        worker.shutdown();
      }
      return;
    }
    if ( !shutdownStarted.compareAndSet( false, true ) ) {
      return;
    }
    // priming listings BEFORE the actor drain: a listing submits its
    // PrimeStarEvent to the actor, and with the actor already gone every
    // drained listing failed pointlessly (30s each, one WARN per star)
    drainPendingPrimingListings();
    // drain the actor first: events still queued on it submit store ops
    // to the cache executor, which must outlive the actor for them
    actor.executor.shutdown();
    awaitTermination( actor.executor );
    // drain the sequenced store chains: a chain link submits only when its
    // predecessor finishes - shutting the pool first rejected late links
    // and lost queued removes (ghost entries in external stores)
    awaitPendingCacheWrites();
    cacheExecutor.shutdown();
    sqlExecutor.shutdown();
    // after the actor drain: index.cancel submits its cancel tasks ON the
    // actor, so nothing can enqueue here any more
    statementCancelExecutor.shutdown();
    awaitTermination( cacheExecutor );
    awaitTermination( sqlExecutor );
    awaitTermination( statementCancelExecutor );
    // tear down only the local in-memory store; external caches are shared
    // across instances and owned by their providers — detach, never tearDown
    for ( SegmentCacheWorker worker : segmentCacheWorkers ) {
      if ( !externalCacheListeners.containsKey( worker.cache ) ) {
        worker.shutdown();
      }
    }
    segmentCacheWorkers.clear();
    externalCacheListeners.forEach( SegmentCache::removeListener );
    externalCacheListeners.clear();
  }

  /**
   * Waits until every queued external cache write or remove has run.
   * Completed chains remove themselves, so this drains to quiescence;
   * writes race in from queries still running elsewhere.
   */
  public void awaitPendingCacheWrites() {
    // priming listings first: their PrimeStarEvents are then queued on the
    // actor FIFO ahead of any later lookup command
    drainPendingPrimingListings();
    storeOps.awaitQuiescence();
  }

  private void drainPendingPrimingListings() {
    for ( java.util.concurrent.Future<?> listing;
          ( listing = pendingPrimingListings.poll() ) != null; ) {
      try {
        listing.get( 30, TimeUnit.SECONDS );
      } catch ( InterruptedException e ) {
        Thread.currentThread().interrupt();
        // break, never return: the caller still owes the store-chain
        // drain - skipping it dropped queued external removes
        break;
      } catch ( Exception e ) {
        // listing failures are logged where they occur
      }
    }
  }

  private static void awaitTermination( ExecutorService executor ) {
    try {
      if ( !executor.awaitTermination( 30, TimeUnit.SECONDS ) ) {
        executor.shutdownNow();
      }
    } catch ( InterruptedException e ) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public SegmentBuilder.SegmentConverter getConverter(
    RolapStar star,
    SegmentHeader header ) {
    return getConverter( star, header.factKey() );
  }

  /**
   * A header carries everything needed to convert: the runtime predicates
   * rebuild from the wire form, the measure resolves by name. Null when a
   * predicate has no structural mapping (opaque) — such a segment is not
   * servable on this node and reloads instead.
   */
  public SegmentBuilder.SegmentConverter getConverter(
    RolapStar star,
    SegmentIdentity.FactKey key ) {
    if ( star == null ) {
      // no catalog holds this star (yet): the caller falls back to its
      // request-scoped converter map
      return null;
    }
    // positive results memoized per star: peek walks every header through
    // here. Misses stay uncached - the catalog build can add measures, and
    // an unresolvable predicate today may resolve after a reload.
    final Map<SegmentIdentity.FactKey, SegmentBuilder.SegmentConverter> perStar =
        converterMemo.computeIfAbsent( star, s -> new ConcurrentHashMap<>() );
    final SegmentBuilder.SegmentConverter cached = perStar.get( key );
    if ( cached != null ) {
      return cached;
    }
    RolapStar.Measure measure =
        star.getFactTable().lookupMeasureByName( key.cubeName(), key.measureName() );
    if ( measure == null ) {
      return null;
    }
    final SegmentBuilder.SegmentConverter converter =
        SegmentPredicates.toStarPredicates( key.compoundPredicates(), star )
            .<SegmentBuilder.SegmentConverter>map(
                predicates -> new SegmentBuilder.StarSegmentConverter( measure, predicates ) )
            .orElse( null );
    if ( converter != null ) {
      perStar.put( key, converter );
    }
    return converter;
  }

  /**
   * Makes a quick request to the aggregation manager to see whether the cell value required by a particular cell
   * request is in external cache.
   *
   * One synchronous actor round-trip; the body fetch from the composite
   * cache happens afterwards on the caller's thread. If the segment is in
   * cache this saves batching up the request and re-executing the query -
   * without it, every query would require at least two iterations.
   *
   * Request does not issue SQL to populate the segment. Nor does it
   * try to find existing segments for rollup. Those operations can wait until next phase.
   *
   * Client is responsible for adding the segment to its private cache.
   *
   * @param request Cell request
   * @return Segment with data, or null if not in cache
   */
  public SegmentWithData peek( final CellRequest request ) {
    // Use currentOrNull() as peek may be called from contexts without execution context
    // (e.g., virtual cubes, background cache operations)
    ExecutionContext executionContext = ExecutionContext.currentOrNull();
    final RolapStar star = request.getMeasure().getStar();
    final Map<SegmentHeader, Future<SegmentBody>> headerMap =
      execute(
        new PeekCommand( request, executionContext) );
    // converters reconstruct purely from the header — on this thread,
    // not on the actor
    for ( SegmentHeader header : headerMap.keySet() ) {
      final SegmentBody body = compositeCache.get( header );
      if ( body != null ) {
        final SegmentBuilder.SegmentConverter converter = getConverter( star, header );
        if ( converter != null ) {
          try {
            return converter.convert( header, body );
          } catch ( IllegalArgumentException e ) {
            // store-read header with bits this star does not know: drop
            // it and keep peeking instead of aborting the query
            LOGGER.warn( "dropping unconvertible store segment {}", header.getUniqueID(), e );
            // NOT remove(star, header): that builds EventContext.current()
            // and throws for the context-free callers peek explicitly
            // supports (virtual cubes, background cache ops)
            actor.event( this, new SegmentRemoveEvent(
                executionContext != null
                    ? EventContext.current()
                    : EventContext.external( context ),
                star, header ) );
          }
        }
      }
    }
    for ( Map.Entry<SegmentHeader, Future<SegmentBody>> entry
      : headerMap.entrySet() ) {
      final Future<SegmentBody> bodyFuture = entry.getValue();
      if ( bodyFuture != null ) {
        final SegmentHeader header = entry.getKey();
        // converter check BEFORE the wait: it is a pure function of the
        // header, and an unconvertible one otherwise cost the full
        // foreign-load latency just to discard the body
        final SegmentBuilder.SegmentConverter converter = getConverter( star, header );
        if ( converter == null ) {
          continue;
        }
        final SegmentBody body =
          awaitPeekedBody( bodyFuture, executionContext );
        try {
          return converter.convert( header, body );
        } catch ( IllegalArgumentException e ) {
          // same guard as the store loop above: an alien header must not
          // abort the peeking query - keep trying the remaining futures
          LOGGER.warn( "dropping unconvertible in-flight segment {}", header.getUniqueID(), e );
        }
      }
    }
    return null;
  }

  /**
   * Waits for someone else's in-flight load of a peeked segment. Sliced
   * and cancel-aware like the query path's waits - the old unbounded
   * safeGet pinned the peeking user thread forever when the loading query
   * ran against a wedged database (the shepherd only cancels the
   * FutureTask, never this latch). Without an execution context the wait
   * is bounded at 30 seconds. Unwrap semantics mirror Util.safeGet.
   */
  private SegmentBody awaitPeekedBody( Future<SegmentBody> bodyFuture,
      ExecutionContext executionContext ) {
    final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos( 30 );
    while ( true ) {
      if ( executionContext != null ) {
        executionContext.checkCancelOrTimeout();
        final Execution execution = executionContext.getExecution();
        if ( execution != null ) {
          execution.checkCancelOrTimeout();
        }
      } else if ( System.nanoTime() - deadlineNanos >= 0 ) {
        throw Util.newError( "timed out waiting for a peeked segment to load" );
      }
      try {
        return bodyFuture.get( 1, TimeUnit.SECONDS );
      } catch ( TimeoutException e ) {
        // slice elapsed - re-check cancellation and keep waiting
      } catch ( InterruptedException e ) {
        Thread.currentThread().interrupt();
        throw Util.newError( e, "Waiting for segment to load" );
      } catch ( ExecutionException e ) {
        Throwable cause = e.getCause();
        if ( cause instanceof RuntimeException runtimeException ) {
          throw runtimeException;
        }
        if ( cause instanceof Error error ) {
          throw error;
        }
        throw Util.newError( cause, "Waiting for segment to load" );
      }
    }
  }

  /**
   * The identity every monitor event carries: when it happened, where it
   * goes, and which server/connection/statement/execution produced it.
   */
  private record EventContext( Instant timestamp, EventBus monitor, String serverId,
      long connectionId, long statementId, long executionId ) {

    /** The identity of the current execution. */
    static EventContext current() {
      final ExecutionContext executionContext = ExecutionContext.current();
      final var statement = executionContext.getExecution().getDaanseStatement();
      return new EventContext(
          Instant.now(),
          statement.getDaanseConnection().getContext().getMonitor(),
          statement.getDaanseConnection().getContext().getName(),
          statement.getDaanseConnection().getId(),
          statement.getId(),
          executionContext.getExecution().getId() );
    }

    /** An externally triggered event: no execution to attribute it to. */
    static EventContext external( Context<?> context ) {
      return new EventContext(
          Instant.now(), context.getMonitor(), context.getName(), 0, 0, 0 );
    }
  }

  private static CellCacheEventCommon cellCacheEventCommon(
      EventContext eventContext, CellCacheEvent.Source source ) {
    return new CellCacheEventCommon(
        new ExecutionEventCommon(
            new MdxStatementEventCommon(
                new ConnectionEventCommon(
                    new ServertEventCommon(
                        new EventCommon( eventContext.timestamp() ),
                        eventContext.serverId() ),
                    eventContext.connectionId() ),
                eventContext.statementId() ),
            eventContext.executionId() ),
        source );
  }
  /**
   * Command to flush a particular region from cache.
   */
  public static final class FlushCommand implements CacheCommand<FlushResult> {
    private final CellRegion region;
    private final CacheControlImpl cacheControlImpl;
    private final ExecutionContext executionContext;
    private final SegmentCacheManager cacheMgr;
    // trace output is buffered here and printed by the CALLER: the command
    // runs on the actor thread, which must never block on the caller's
    // PrintWriter (a slow or interactive sink would stall every cache op)
    private final List<String> traceMessages = new ArrayList<>();

    public FlushCommand(
      ExecutionContext executionContext,
      SegmentCacheManager mgr,
      CellRegion region,
      CacheControlImpl cacheControlImpl ) {
      this.executionContext = executionContext;
      this.cacheMgr = mgr;
      this.region = region;
      this.cacheControlImpl = cacheControlImpl;
    }

    @Override
	public ExecutionContext getExecutionContext() {
      return executionContext;
    }

    @Override
	public FlushResult call() {
      final List<Member> measures = CacheControlImpl.findMeasures( region );
      final SegmentColumn[] flushRegion = CacheControlImpl.findAxisValues( region );
      final List<RolapStar> starList = CacheControlImpl.getStarList( region );

      final List<Pair<RolapStar, SegmentHeader>> headers = getIntersectingHeaders( measures, flushRegion );

      // If flushRegion is empty, this means we must clear all
      // segments for the region's measures.
      if ( flushRegion.length == 0 ) {
        return new FlushResult(
          clearAllSegmentsForRegionsMeasures( starList, headers ), traceMessages );
      }
      return getFlushResult( flushRegion, starList, headers );

    }

    private FlushResult getFlushResult( SegmentColumn[] flushRegion, List<RolapStar> starList,
                                        List<Pair<RolapStar, SegmentHeader>> headers ) {
      // Now we know which headers intersect. For each of them,
      // we append an excluded region.
      //
      // TODO: Optimize the logic here. If a segment is mostly
      // empty, we should trash it completely.
      final List<Supplier<CompletableFuture<Boolean>>> taskList =
        new ArrayList<>();
      for ( final Pair<RolapStar, SegmentHeader> pair : headers ) {
        final SegmentHeader header = pair.right;
        final SegmentCacheIndex index = cacheMgr.indexRegistry.getIndex( pair.left );
        if ( !header.canConstrain( flushRegion ) ) {
          // We have to delete that segment altogether - from the STORES
          // too: an index-only remove left the entry behind, and the next
          // priming or attach re-indexed the flushed segment (the flush
          // was not persistent). Same task shape as the empty-region path.
          if ( cacheControlImpl.isTraceEnabled() ) {
            traceMessages.add(
              "discard segment - it cannot be constrained and maintain consistency:\n"
                + header.getDescription() );
          }
          index.remove( header );
          taskList.add(
            // the workers degrade and log their own store failures; a
            // throw out of here is unexpected and must surface to the
            // caller's balance as an exceptional completion, not TRUE
            () -> cacheMgr.sequencedCacheOp( header,
              () -> cacheMgr.compositeCache.remove( header ) ) );
          continue;
        }

        // Build the new header's dimensionality
        final SegmentHeader newHeader = header.constrain( flushRegion );
        if ( newHeader == header ) {
          // the box is already excluded (an idempotent re-flush):
          // nothing to update, rename or publish
          continue;
        }
        index.update( header, newHeader );
        // Update all of the cache workers.
        clearCacheWorkers( taskList, header, newHeader );
      }
      for ( RolapStar star : starList ) {
        star.invalidateWorkingStores();
      }
      return new FlushResult( taskList, traceMessages );
    }

    private void clearCacheWorkers( List<Supplier<CompletableFuture<Boolean>>> taskList,
                                    SegmentHeader header, SegmentHeader newHeader ) {
      for ( final SegmentCacheWorker worker
        : cacheMgr.segmentCacheWorkers ) {
        taskList.add(
          // sequenced under BOTH ids: the target side had no chain, so a
          // rename chain H->H1, H1->H2 could execute out of order and leave
          // a ghost segment under the intermediate id
          () -> cacheMgr.sequencedCacheOp( header, newHeader, () -> {
            // the body is re-keyed inside the store where the store supports
            // it (JDBC UPDATE, Redis RENAME); a remote Hazelcast client pulls
            // the bytes over get and writes them back under the new key
            boolean renamed = worker.rename( header, newHeader );
            if ( renamed ) {
              // the OLD header is dead everywhere now - tell the same-JVM
              // neighbor catalogs (their indexes still hold it; the store
              // fires no same-JVM event for its own rename). Idempotent
              // with a store-level death event (Redis collision publish);
              // the monitor may count the delete twice, like the create.
              cacheMgr.externalSegmentDeleted( header, cacheMgr.context );
              // the store fires no same-JVM CREATE for its own rename
              // (Hazelcast filters the self-echo, JDBC has no events at
              // all): the manager publishes the birth itself, so neighbor
              // catalogs sharing this JVM index the constrained header.
              // Publication goes to THIS manager only - an overlay's
              // rename never reaches the shared manager's indexes. The
              // index add is idempotent for the flushing star (update
              // already adopted the header); the monitor may count this
              // segment's creation a second time.
              cacheMgr.externalSegmentCreated( newHeader, cacheMgr.context );
            }
            return renamed;
          } ) );
      }
    }

    private List<Supplier<CompletableFuture<Boolean>>> clearAllSegmentsForRegionsMeasures(
        List<RolapStar> starList, List<Pair<RolapStar, SegmentHeader>> headers ) {
      for ( RolapStar star : starList ) {
        star.invalidateWorkingStores();
      }
      // External removes go back to the caller as tasks — the actor thread
      // must never wait on a store round-trip.
      final List<Supplier<CompletableFuture<Boolean>>> taskList = new ArrayList<>();
      for ( final Pair<RolapStar, SegmentHeader> pair : headers ) {
        final SegmentHeader header = pair.right;
        cacheMgr.indexRegistry.getIndex( pair.left ).remove( header );
        if ( cacheControlImpl.isTraceEnabled() ) {
          traceMessages.add(
            "discard segment - it cannot be constrained and maintain consistency:\n"
              + header.getDescription() );
        }
        taskList.add(
          // workers degrade and log their own store failures; a throw is
          // unexpected and surfaces as an exceptional completion
          () -> cacheMgr.sequencedCacheOp( header,
            () -> cacheMgr.compositeCache.remove( header ) ) );
      }
      return taskList;
    }

    /**
     * For each measure and each star, ask the index
     * which headers intersect.
     */
    private List<Pair<RolapStar, SegmentHeader>> getIntersectingHeaders(
        List<Member> measures, SegmentColumn[] flushRegion ) {
      final List<Pair<RolapStar, SegmentHeader>> headers =
        new ArrayList<>();
      for ( Member member : measures ) {
        if ( !( member instanceof RolapStoredMeasure storedMeasure ) ) {
          continue;
        }
        final RolapStar star = storedMeasure.getCube().getStar();
        final SegmentCacheIndex index =
          cacheMgr.indexRegistry.getIndex( star );
        for ( SegmentHeader header : index.intersectRegion(
            new SegmentIdentity.RegionKey(
              member.getDimension().getCatalog().getName(),
              ( (RolapCatalog) member.getDimension().getCatalog() )
                .getChecksum(),
              storedMeasure.getCube().getName(),
              storedMeasure.getCube().getStar()
                .getFactTable().getAlias(),
              storedMeasure.getName() ),
            flushRegion ) ) {
          headers.add( Pair.of( star, header ) );
        }
      }
      if ( cacheControlImpl.isTraceEnabled() ) {
        headers.sort( Comparator.comparing( p -> p.right.getUniqueID() ) );
      }
      return headers;
    }
  }

  private class PrintCacheStateCommand implements CacheCommand<String> {
    private final ExecutionContext executionContext;
    private final CellRegion region;

    public PrintCacheStateCommand(
      CellRegion region,
      ExecutionContext executionContext ) {
      this.region = region;
      this.executionContext = executionContext;
    }

    @Override
	public String call() {
      final StringWriter buffer = new StringWriter();
      final PrintWriter out = new PrintWriter( buffer );
      final List<RolapStar> starList =
        CacheControlImpl.getStarList( region );
      starList.sort( Comparator.comparing( o -> o.getFactTable().getAlias() ) );
      for ( RolapStar star : starList ) {
        indexRegistry.getIndex( star )
          .printCacheState( out );
      }
      out.flush();
      return buffer.toString();
    }

    @Override
	public ExecutionContext getExecutionContext() {
      return executionContext;
    }
  }

  /**
   * Result of a {@link FlushCommand}. Contains a list of tasks that must be executed by the caller (or by an executor)
   * to flush segments from the external cache(s).
   */
  public static class FlushResult {
    public final List<Supplier<CompletableFuture<Boolean>>> tasks;
    /** Buffered trace output, printed by the caller off the actor thread. */
    public final List<String> traceMessages;

    public FlushResult( List<Supplier<CompletableFuture<Boolean>>> tasks ) {
      this( tasks, List.of() );
    }

    public FlushResult( List<Supplier<CompletableFuture<Boolean>>> tasks, List<String> traceMessages ) {
      this.tasks = tasks;
      this.traceMessages = traceMessages;
    }
  }


  private abstract static class Event {

    /** Applies this event to the manager's state, on the actor thread. */
    abstract void run( SegmentCacheManager cacheMgr );
  }

  /**
   * Single-thread executor that confines every index mutation to one actor
   * thread; commands answer synchronously, events run fire-and-forget.
   */
  private static class Actor {

    private static final AtomicInteger NAME_COUNTER = new AtomicInteger();

    final ExecutorService executor;
    final Thread thread;

    Actor() {
      final Thread[] holder = new Thread[1];
      // suffixed like the pools' names (Util appends _n there)
      final String name = "daanse.rolap.agg.SegmentCacheManager$ACTOR_" + NAME_COUNTER.incrementAndGet();
      executor = Executors.newSingleThreadExecutor( r -> {
        Thread t = new Thread( r, name );
        t.setDaemon( true );
        holder[0] = t;
        return t;
      } );
      // materialize the thread so index guards can compare against it
      try {
        executor.submit( () -> { } ).get();
      } catch ( InterruptedException e ) {
        Thread.currentThread().interrupt();
      } catch ( ExecutionException e ) {
        // empty task
      }
      thread = holder[0];
    }

    <T> T execute( CacheCommand<T> command ) {
      try {
        return executor.submit( () -> {
          ExecutionContext ctx = command.getExecutionContext();
          if ( ctx != null ) {
            return ExecutionContext.where( ctx, () -> {
              return command.call();
            } );
          }
          return command.call();
        } ).get();
      } catch ( InterruptedException e ) {
        Thread.currentThread().interrupt();
        throw Util.newError( e, "Exception while executing " + command );
      } catch ( ExecutionException e ) {
        final Throwable cause = e.getCause();
        if ( cause instanceof RuntimeException runtime ) {
          throw runtime;
        }
        if ( cause instanceof Error error ) {
          throw error;
        }
        throw new IllegalStateException( cause );
      } catch ( RejectedExecutionException e ) {
        // answer after the pre-shutdown work: callers see completed commands
        // first, then the rejection
        try {
          executor.awaitTermination( 30, TimeUnit.SECONDS );
        } catch ( InterruptedException interrupted ) {
          Thread.currentThread().interrupt();
        }
        throw Util.newError( "Command submitted after shutdown " + command );
      }
    }

    void event( SegmentCacheManager cacheMgr, Event event ) {
      try {
        executor.execute( () -> {
          try {
            event.run( cacheMgr );
            RolapUtil.MONITOR_LOGGER.debug( "{}", event );
          } catch ( Throwable t ) {
            // Errors included: an escaping Error kills the worker, the
            // executor replaces it, and both thread guards (index assert,
            // worker rejection) keep comparing against the dead original.
            // The one actor thread must never die. execute() is immune the
            // same way via FutureTask.
            LOGGER.error( "event failed: " + event, t );
          }
        } );
      } catch ( RejectedExecutionException e ) {
        throw Util.newError( "Event submitted after shutdown " + event );
      }
    }
  }
  private static class SegmentLoadSucceededEvent extends Event {
    private final EventContext context;
    private final RolapStar star;
    private final SegmentHeader header;
    private final SegmentBody body;
    private final CellCacheEvent.Source source;

    SegmentLoadSucceededEvent(
      EventContext context,
      RolapStar star,
      SegmentHeader header,
      SegmentBody body,
      CellCacheEvent.Source source ) {
      assert header != null;
      assert star != null;
      this.context = context;
      this.star = star;
      this.header = header;
      this.body = body; // may be null
      this.source = source;
    }

    @Override
    public String toString() {
      return "SegmentLoadSucceededEvent(" + header.getUniqueID() + ", source=" + source + ")";
    }

    @Override
    void run( SegmentCacheManager cacheMgr ) {
      final SegmentCacheIndex index = cacheMgr.indexRegistry.getIndex( star );
      // a header the index no longer holds - or holds only flagged for
      // removal after this very load (a flush hit it mid-flight) - arrived
      // late: its body must NOT reach the stores, it is the PRE-flush data
      // under the old id. contains() alone missed the removeAfterLoad case
      // and leaked exactly that ghost.
      final boolean registered = index.isRegistered( header );
      publishToIndex( index, header, body );
      context.monitor().accept( new CellCacheSegmentCreateEvent(
          cellCacheEventCommon( context, source ),
          header.getConstrainedColumns().size(),
          body == null ? 0 : body.cellCount() ) );
      if ( !registered ) {
        LOGGER.debug( "skipping store put for late segment {}", header.getUniqueID() );
        return;
      }
      // enqueue only - sequenced per segment id on the cache executor,
      // the actor never waits on the store round-trip
      cacheMgr.sequencedCacheOp( header, () -> {
        cacheMgr.compositeCache.put( header, body );
        return null;
      } ).whenComplete( ( v, t ) -> {
        if ( t != null ) {
          LOGGER.warn( "external cache put failed for " + header.getUniqueID(), t );
        }
      } );
    }
  }

  /**
   * The index's loadSucceeded is the only completer of a pending load
   * slot; if it throws, every query waiting on the slot hangs until its
   * timeout. Failing the slot hands the waiters the cause instead.
   */
  static void publishToIndex( SegmentCacheIndex index, SegmentHeader header, SegmentBody body ) {
    try {
      index.loadSucceeded( header, body );
    } catch ( RuntimeException | Error e ) {
      try {
        index.loadFailed( header, e );
      } catch ( RuntimeException | Error failFailed ) {
        LOGGER.error( "failing the load slot after a failed publication also failed", failFailed );
      }
      throw e;
    }
  }

  private static class SegmentLoadFailedEvent extends Event {
    private final RolapStar star;
    private final SegmentHeader header;
    private final Throwable throwable;

    SegmentLoadFailedEvent(
      RolapStar star,
      SegmentHeader header,
      Throwable throwable ) {
      assert header != null;
      this.star = star;
      this.header = header;
      this.throwable = throwable;
    }

    @Override
    public String toString() {
      return "SegmentLoadFailedEvent(" + header.getUniqueID() + ", " + throwable + ")";
    }

    @Override
    void run( SegmentCacheManager cacheMgr ) {
      cacheMgr.indexRegistry.getIndex( star ).loadFailed( header, throwable );
    }
  }

  private static class SegmentRemoveEvent extends Event {
    private final EventContext context;
    private final RolapStar star;
    private final SegmentHeader header;

    SegmentRemoveEvent(
      EventContext context,
      RolapStar star,
      SegmentHeader header ) {
      assert header != null;
      this.context = context;
      this.star = star;
      this.header = header;
    }

    @Override
    public String toString() {
      return "SegmentRemoveEvent(" + header.getUniqueID() + ")";
    }

    @Override
    void run( SegmentCacheManager cacheMgr ) {
      cacheMgr.indexRegistry.getIndex( star ).remove( header );
      star.invalidateWorkingStores();
      context.monitor().accept( new CellCacheSegmentDeleteEvent(
          cellCacheEventCommon( context, CellCacheEvent.Source.CACHE_CONTROL ),
          header.getConstrainedColumns().size() ) );

      // Remove the segment from external caches, ordered against any put
      // still queued for the same segment id. Not awaited — this runs on
      // the actor thread, which must never wait on a store round-trip.
      cacheMgr.sequencedCacheOp( header, () -> {
        try {
          cacheMgr.compositeCache.remove( header );
        } catch ( Exception e ) {
          LOGGER.warn( "remove header failed: " + header, e );
        }
        return null;
      } );
    }
  }

  private static class ExternalSegmentCreatedEvent extends Event {
    private final EventContext context;
    private final SegmentHeader header;

    ExternalSegmentCreatedEvent(
      EventContext context,
      SegmentHeader header ) {
      assert header != null;
      this.context = context;
      this.header = header;
    }

    @Override
    public String toString() {
      return "ExternalSegmentCreatedEvent(" + header.getUniqueID() + ")";
    }

    @Override
    void run( SegmentCacheManager cacheMgr ) {
      // content-identical catalogs opened under different connections each
      // hold their own index over the same segment ids: the external create
      // applies to every one of them (symmetric to the deleted event)
      List<RolapStar> stars = cacheMgr.getStars( header );
      if ( stars.isEmpty() ) {
        // this catalog is not loaded here (yet). Mark the fact table so a
        // later star build primes from the external inventory and picks
        // this header up along with everything else.
        cacheMgr.factTablesToPrime.add( new SegmentCache.StarKey(
            header.schemaChecksum.toString(), header.rolapStarFactTableName ) );
        // double-check: a concurrent getOrCreateStar may have registered
        // the star between the miss above and the add - its priming ran
        // before the marker existed, and every store header of this star
        // (this one included) would stay invisible until a catalog
        // rebuild. Re-priming with the fresh marker picks them all up.
        stars = cacheMgr.getStars( header );
        if ( stars.isEmpty() ) {
          LOGGER.debug(
            "SegmentCacheManager.ExternalSegmentCreatedEvent:No star loaded for external SegmentHeader:{}",
              header );
          return;
        }
        for ( RolapStar star : stars ) {
          // the first star consumes the marker and lists the full store
          // inventory (this header included); a star primed earlier just
          // needs this one header
          if ( !cacheMgr.schedulePrimingIfPending( star ) ) {
            cacheMgr.indexRegistry.getIndex( star ).add( header, false );
          }
        }
        // honest events: this recovery branch indexes the header too
        context.monitor().accept( new CellCacheSegmentCreateEvent(
            cellCacheEventCommon( context, CellCacheEvent.Source.EXTERNAL ),
            header.getConstrainedColumns().size(), 0 ) );
        return;
      }
      for ( RolapStar star : stars ) {
        cacheMgr.indexRegistry.getIndex( star ).add( header, false );
      }
      context.monitor().accept( new CellCacheSegmentCreateEvent(
          cellCacheEventCommon( context, CellCacheEvent.Source.EXTERNAL ),
          header.getConstrainedColumns().size(), 0 ) );
    }
  }

  /** Feeds pre-existing external headers into a star's index on the actor thread. */
  private static class PrimeStarEvent extends Event {
    private final EventContext context;
    private final RolapStar star;
    private final List<SegmentHeader> headers;

    PrimeStarEvent(
      EventContext context,
      RolapStar star,
      List<SegmentHeader> headers ) {
      this.context = context;
      this.star = star;
      this.headers = List.copyOf( headers );
    }

    @Override
    public String toString() {
      return "PrimeStarEvent(" + star.getFactTable().getAlias() + ", " + headers.size() + " header(s))";
    }

    @Override
    void run( SegmentCacheManager cacheMgr ) {
      if ( BitKeyExplain.enabled() ) {
        BitKeyExplain.EXPLAIN.debug( "prime star {} with {} external header(s)",
            star.getFactTable().getAlias(), headers.size() );
      }
      final SegmentCacheIndex index = cacheMgr.indexRegistry.getIndex( star );
      for ( SegmentHeader header : headers ) {
        index.add( header, false );
        context.monitor().accept( new CellCacheSegmentCreateEvent(
            cellCacheEventCommon( context, CellCacheEvent.Source.EXTERNAL ),
            header.getConstrainedColumns().size(), 0 ) );
      }
    }
  }

  private static class ExternalSegmentDeletedEvent extends Event {
    private final EventContext context;
    private final SegmentHeader header;

    ExternalSegmentDeletedEvent(
      EventContext context,
      SegmentHeader header ) {
      assert header != null;
      this.context = context;
      this.header = header;
    }

    @Override
    public String toString() {
      return "ExternalSegmentDeletedEvent(" + header.getUniqueID() + ")";
    }

    @Override
    void run( SegmentCacheManager cacheMgr ) {
      // content-identical catalogs opened under different connections each
      // hold their own index over the same segment ids: the external delete
      // applies to every one of them
      final List<RolapStar> deletedStars = cacheMgr.getStars( header );
      if ( deletedStars.isEmpty() ) {
        LOGGER.debug(
          "SegmentCacheManager.ExternalSegmentDeletedEvent:No index found for external SegmentHeader:",
            header );
        return;
      }
      for ( RolapStar deletedStar : deletedStars ) {
        cacheMgr.indexRegistry.getIndex( deletedStar ).remove( header );
        deletedStar.invalidateWorkingStores();
      }
      context.monitor().accept( new CellCacheSegmentDeleteEvent(
          cellCacheEventCommon( context, CellCacheEvent.Source.EXTERNAL ),
          header.getConstrainedColumns().size() ) );
    }
  }

  /**
   * Implementation of SegmentCacheListener that updates the segment index of its aggregation manager instance when it
   * receives events from its assigned SegmentCache implementation.
   */
  private static class AsyncCacheListener
    implements SegmentCache.SegmentCacheListener {
    private final SegmentCacheManager cacheMgr;
    private final Context context;

    public AsyncCacheListener(
      SegmentCacheManager cacheMgr,
      Context context ) {
      this.cacheMgr = cacheMgr;
      this.context = context;
    }

    @Override
	public void handle( final SegmentCacheEvent e ) {
      if ( e.isLocal() ) {
        return;
      }
      // enqueue only — this runs on the store's I/O thread, which must
      // not wait for the actor. After shutdown the enqueue throws; that
      // must not escape into the store's event thread (Redis pub/sub,
      // Hazelcast listener threads react badly to foreign exceptions)
      try {
        switch ( e.getEventType() ) {
          case ENTRY_CREATED:
            cacheMgr.externalSegmentCreated( e.getSource(), context );
            break;
          case ENTRY_DELETED:
            cacheMgr.externalSegmentDeleted( e.getSource(), context );
            break;
          default:
            throw new UnsupportedOperationException();
        }
      } catch ( RuntimeException ex ) {
        LOGGER.warn( "dropped external cache event after shutdown or failure", ex );
      }
  }
  }

  /**
   * Makes a collection of {@link SegmentCacheWorker} objects (each of which is backed by a {@link SegmentCache} appear
   * to be a SegmentCache.
   *
   * For most operations, it is easier to operate on a single cache.
   * It is usually clear whether operations should quit when finding the first match, or to operate on all workers. (For
   * example, {@link #remove} tries to remove the segment header from all workers, and returns whether it was removed
   * from any of them.) This class just does what seems most typical. If you want another behavior for a particular
   * operation, operate on the workers directly.
   */
  public static class CompositeSegmentCache implements SegmentCache {
    final List<SegmentCacheWorker> workers;
    final boolean disableCaching;

    public CompositeSegmentCache( List<SegmentCacheWorker> workers, boolean disableCaching ) {
      this.workers = workers;
      this.disableCaching = disableCaching;
    }

    @Override
	public SegmentBody get( SegmentHeader header ) {
      for ( SegmentCacheWorker worker : workers ) {
        final SegmentBody body = worker.get( header );
        if ( body != null ) {
          if ( BitKeyExplain.enabled() ) {
            BitKeyExplain.EXPLAIN.debug( "fetch body from {} for {}",
                worker.cacheName(), BitKeyExplain.explain( header ) );
          }
          return body;
        }
      }
      if ( BitKeyExplain.enabled() && !workers.isEmpty() ) {
        BitKeyExplain.EXPLAIN.debug( "no external store holds {}",
            BitKeyExplain.explain( header ) );
      }
      return null;
    }

    @Override
	public List<SegmentHeader> getSegmentHeaders() {
      if ( disableCaching ) {
        return Collections.emptyList();
      }
      // Special case 0 and 1 workers, for which the 'union' operation
      // is trivial.
      switch ( workers.size() ) {
        case 0:
          return Collections.emptyList();
        case 1:
          return workers.get( 0 ).getSegmentHeaders();
        default:
          final List<SegmentHeader> list = new ArrayList<>();
          final Set<SegmentHeader> set = new HashSet<>();
          for ( SegmentCacheWorker worker : workers ) {
            for ( SegmentHeader header : worker.getSegmentHeaders() ) {
              if ( set.add( header ) ) {
                list.add( header );
              }
            }
          }
          return list;
      }
    }

    @Override
	public List<SegmentHeader> getSegmentHeaders(
        ByteString schemaChecksum,
        String rolapStarFactTableName ) {
      if ( disableCaching ) {
        return Collections.emptyList();
      }
      final List<SegmentHeader> list = new ArrayList<>();
      final Set<SegmentHeader> set = new HashSet<>();
      for ( SegmentCacheWorker worker : workers ) {
        for ( SegmentHeader header
            : worker.getSegmentHeaders( schemaChecksum, rolapStarFactTableName ) ) {
          if ( set.add( header ) ) {
            list.add( header );
          }
        }
      }
      return list;
    }

    // this method always returns true, but return value needed by api.
    @Override
	@SuppressWarnings( "squid:S3516" )
    public boolean put( SegmentHeader header, SegmentBody body ) {
      if ( disableCaching ) {
        return true;
      }
      for ( SegmentCacheWorker worker : workers ) {
        worker.put( header, body );
      }
      return true;
    }

    @Override
	public boolean remove( SegmentHeader header ) {
      boolean result = false;
      for ( SegmentCacheWorker worker : workers ) {
        if ( worker.remove( header ) ) {
          result = true;
        }
      }
      return result;
    }

    @Override
	public void tearDown() {
      for ( SegmentCacheWorker worker : workers ) {
        worker.shutdown();
      }
    }

    @Override
	public void addListener( SegmentCacheListener listener ) {
      for ( SegmentCacheWorker worker : workers ) {
        worker.cache.addListener( listener );
      }
    }

    @Override
	public void removeListener( SegmentCacheListener listener ) {
      for ( SegmentCacheWorker worker : workers ) {
        worker.cache.removeListener( listener );
      }
    }

  }

  /**
   * Locates segments in the cache that satisfy a given request.
   *
   * The result consists of (a) a list of segment headers, (b) a list
   * of futures for segment bodies that are currently being loaded, (c) converters to convert headers into {@link
   * SegmentWithData}.
   *
   * For (a), the client should call the cache to get the body for each
   * segment header; it is possible that there is no body in the cache. For (b), the client will have to wait for the
   * segment to arrive.
   */
  private class PeekCommand implements CacheCommand<Map<SegmentHeader, Future<SegmentBody>>> {
    private final CellRequest request;
    private final ExecutionContext executionContext;

    /**
     * Creates a PeekCommand.
     *
     * @param request Cell request
     * @param executionContext   ExecutionContext
     */
    public PeekCommand(
      CellRequest request,
      ExecutionContext executionContext ) {
      this.request = request;
      this.executionContext = executionContext;
    }

    @Override
	public Map<SegmentHeader, Future<SegmentBody>> call() {
      final RolapStar star = request.getMeasure().getStar();
      final SegmentCacheIndex index = indexRegistry.getIndex( star );
      final List<SegmentHeader> headers =
        index.locate( request.segmentIdentity(), request.getMappedCellValues() );

      final Map<SegmentHeader, Future<SegmentBody>> headerMap =
        new HashMap<>();

      // every located header goes to the client: with a future when a SQL
      // load is pending (the client waits), without one when the segment is
      // already loaded (the client asks the composite cache for the body)
      for ( final SegmentHeader header : headers ) {
        final Future<SegmentBody> bodyFuture = executionContext == null ? null
            : index
                .getFuture( executionContext.getExecution(), header );
        headerMap.put(
          header, bodyFuture );
      }

      return headerMap;
    }

    @Override
	public ExecutionContext getExecutionContext() {
      return executionContext;
    }
  }

  /**
   * Registry of all the indexes that were created for this cache manager,
   * one per catalog: all stars of a catalog share the index, entries are
   * disambiguated by their {@code SegmentIdentity}.
   * 
   * The index is based off the checksum of the schema.
   */
  public class SegmentCacheIndexRegistry implements OlapSegmentCacheIndexRegistry{
    private final Map<RolapCatalogKey, SegmentCacheIndex> indexes =
      new ConcurrentHashMap<>();

    /** Drops the index of a catalog that no longer exists (GC-collected). */
    public void dropIndex(RolapCatalogKey key) {
      indexes.remove(key);
    }

    /**
     * Returns the {@link SegmentCacheIndex} for a given {@link RolapStar}.
     */
    public SegmentCacheIndex getIndex( RolapStar star ) {
      final SegmentCacheIndex index = indexes.computeIfAbsent(
        star.getCatalog().getKey(), key -> new SegmentCacheIndexImpl( thread, statementCancelExecutor ) );
      if ( LOGGER.isTraceEnabled() ) {
        LOGGER.trace( "getIndex: star={} index={}",
            System.identityHashCode( star ), System.identityHashCode( index ) );
      }
      return index;
    }

    /** SPI hook, called from ExecutionImpl (olap) when a query dies. */
    @Override
    public void cancelExecutionSegments( Execution exec ) {
      for ( SegmentCacheIndex index : indexes.values() ) {
        index.cancel( exec );
      }
    }
  }

  /**
   * The first loaded star matching the header's content checksum - skipping
   * checksum-matching catalogs that have not built the star yet. Content-
   * identical catalogs under different connections serve identical segments,
   * so any BUILT match converts the header; deletions use {@link #getStars}.
   */
  public RolapStar getStar(SegmentHeader header ) {
    for ( var catalog0 : context.getCatalogCache().getCachedCatalogs() ) {
      RolapCatalog schema = (RolapCatalog) catalog0;
      if ( !schema.getChecksum().equals( header.schemaChecksum ) ) {
        continue;
      }
      // schema match - but stars build lazily: keep scanning when this
      // content-identical catalog has not built the star yet (returning
      // its null aborted the walk and NPEd downstream)
      RolapStar star = schema.getRolapStarRegistry().getStar( header.rolapStarFactTableName );
      if ( star != null ) {
        return star;
      }
    }
    return null;
  }

  /** Every loaded star matching the header's content checksum. */
  public List<RolapStar> getStars( SegmentHeader header ) {
    List<RolapStar> result = new ArrayList<>();
    for ( var catalog0 : context.getCatalogCache().getCachedCatalogs() ) {
      RolapCatalog schema = (RolapCatalog) catalog0;
      if ( schema.getChecksum().equals( header.schemaChecksum ) ) {
        RolapStar star = schema.getRolapStarRegistry().getStar( header.rolapStarFactTableName );
        if ( star != null ) {
          result.add( star );
        }
      }
    }
    return result;
  }

  /**
   * Abandons a segment load whose headers left the index (a flush raced
   * the load); the SQL load path catches it and skips the statement.
   */
  public static final class AbortException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }

    public Context<?> getContext() {
        return context;
    }
}
