/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (c) 2019-2019 Hitachi Vantara.
 * All Rights Reserved.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.eclipse.daanse.olap.api.execution.Execution;
import org.eclipse.daanse.olap.key.BitKey;
import org.eclipse.daanse.olap.spi.SegmentBody;
import org.eclipse.daanse.olap.spi.SegmentColumn;
import org.eclipse.daanse.olap.spi.SegmentHeader;
import org.eclipse.daanse.olap.spi.SegmentIdentity;
import org.eclipse.daanse.olap.util.ByteString;
import org.junit.jupiter.api.Test;

class SegmentCacheIndexImplTest {

    private static final ByteString CHECKSUM = new ByteString("c".getBytes());

    /** Header over columns [f].[a],[f].[b] (bits 0,1), a constrained, b wildcard. */
    private static SegmentHeader header(String aValue) {
        BitKey bitKey = BitKey.Factory.makeBitKey(3);
        bitKey.set(0);
        bitKey.set(1);
        return new SegmentHeader("Schema", CHECKSUM, "Cube", "m1",
            List.of(
                new SegmentColumn("[f].[a]", 10,
                    new TreeSet<>(List.of(aValue))),
                new SegmentColumn("[f].[b]", 10, null)),
            List.of(), "FACT", bitKey, List.of());
    }

    @Test
    void removingOneHeaderKeepsTheDimensionalityOfTheRemaining() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        final SegmentHeader header1 = header("x");
        final SegmentHeader header2 = header("y");
        index.add(header1, false);
        index.add(header2, false);
        index.remove(header1);

        // a coarser request (only bit 0) still rolls up from header2
        BitKey coarse = BitKey.Factory.makeBitKey(3);
        coarse.set(0);
        SegmentIdentity target = new SegmentIdentity("Schema", CHECKSUM, "Cube",
            "FACT", "m1", List.of(), coarse);
        List<List<SegmentHeader>> candidates =
            index.findRollupCandidates(target, Map.of("[f].[a]", "y"));
        assertEquals(1, candidates.size());
        assertTrue(candidates.get(0).contains(header2));

        index.remove(header2);
        assertTrue(index.findRollupCandidates(target, Map.of("[f].[a]", "y")).isEmpty());
    }

    @Test
    void locateSkipsHeadersFlaggedForRemoval() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        final SegmentHeader header = header("x");
        index.add(header, true);
        assertEquals(List.of(header),
            index.locate(header.identity(), Map.of("[f].[a]", "x")));

        // remove during load: flagged stale, gone from locate immediately
        index.remove(header);
        assertTrue(index.locate(header.identity(), Map.of("[f].[a]", "x")).isEmpty());
    }
    /** Header over columns [f].[a],[f].[c] (bits 0,2) — a second shape on the same fact. */
    private static SegmentHeader headerOverAC(String aValue) {
        BitKey bitKey = BitKey.Factory.makeBitKey(3);
        bitKey.set(0);
        bitKey.set(2);
        return new SegmentHeader("Schema", CHECKSUM, "Cube", "m1",
            List.of(
                new SegmentColumn("[f].[a]", 10,
                    new TreeSet<>(List.of(aValue))),
                new SegmentColumn("[f].[c]", 10, null)),
            List.of(), "FACT", bitKey, List.of());
    }

    @Test
    void rollupAncestorsAreMemoizedUntilTheShapeSetChanges() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        final SegmentHeader header1 = header("x");
        index.add(header1, false);
        index.add(header("y"), false);

        BitKey coarse = BitKey.Factory.makeBitKey(3);
        coarse.set(0);
        SegmentIdentity target = new SegmentIdentity("Schema", CHECKSUM, "Cube",
            "FACT", "m1", List.of(), coarse);
        index.findRollupCandidates(target, Map.of("[f].[a]", "x"));

        List<BitKey> cached = index.ancestorMemoForTests(header1).get(coarse);
        assertEquals(1, cached.size());
        index.findRollupCandidates(target, Map.of("[f].[a]", "x"));
        assertSame(cached, index.ancestorMemoForTests(header1).get(coarse));

        // another header of the same shape keeps the memo
        index.add(header("z"), false);
        assertSame(cached, index.ancestorMemoForTests(header1).get(coarse));

        // a new shape clears it; the next lookup sees both dimensionalities
        index.add(headerOverAC("w"), false);
        assertTrue(index.ancestorMemoForTests(header1).isEmpty());
        index.findRollupCandidates(target, Map.of("[f].[a]", "x"));
        assertEquals(2, index.ancestorMemoForTests(header1).get(coarse).size());
    }

    @Test
    void removingTheLastHeaderOfAShapeClearsTheMemo() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        final SegmentHeader header1 = header("x");
        final SegmentHeader header2 = headerOverAC("w");
        index.add(header1, false);
        index.add(header2, false);

        BitKey coarse = BitKey.Factory.makeBitKey(3);
        coarse.set(0);
        SegmentIdentity target = new SegmentIdentity("Schema", CHECKSUM, "Cube",
            "FACT", "m1", List.of(), coarse);
        index.findRollupCandidates(target, Map.of("[f].[a]", "x"));
        assertEquals(2, index.ancestorMemoForTests(header1).get(coarse).size());

        index.remove(header2);
        assertTrue(index.ancestorMemoForTests(header1).isEmpty());
        index.findRollupCandidates(target, Map.of("[f].[a]", "x"));
        assertEquals(1, index.ancestorMemoForTests(header1).get(coarse).size());
    }

    /** Header over [f].[a],[f].[b],[f].[c] (bits 0,1,2) with given b/c value sets (null = wildcard). */
    private static SegmentHeader headerABC(String aValue,
            SortedSet<Comparable> bValues, SortedSet<Comparable> cValues) {
        BitKey bitKey = BitKey.Factory.makeBitKey(3);
        bitKey.set(0);
        bitKey.set(1);
        bitKey.set(2);
        return new SegmentHeader("Schema", CHECKSUM, "Cube", "m1",
            List.of(
                new SegmentColumn("[f].[a]", 10,
                    new TreeSet<>(List.of(aValue))),
                new SegmentColumn("[f].[b]", 2, bValues),
                new SegmentColumn("[f].[c]", 1, cValues)),
            List.of(), "FACT", bitKey, List.of());
    }

    @Test
    void wildcardHeadersCoverEveryValueRegardlessOfAddOrder() {
        // H1 constrains b={x}, c={p}; H2 constrains b={y}, c wildcard. The
        // wildcard must count as covering every known c value, and the
        // candidate result must not depend on the order headers were added.
        SegmentHeader h1 = headerABC("v",
            new TreeSet<>(List.of("x")), new TreeSet<>(List.of("p")));
        SegmentHeader h2 = headerABC("v",
            new TreeSet<>(List.of("y")), null);

        BitKey coarse = BitKey.Factory.makeBitKey(3);
        coarse.set(0);
        SegmentIdentity target = new SegmentIdentity("Schema", CHECKSUM, "Cube",
            "FACT", "m1", List.of(), coarse);

        List<List<SegmentHeader>> forward = candidates(target, h1, h2);
        List<List<SegmentHeader>> backward = candidates(target, h2, h1);
        assertFalse(forward.isEmpty());
        assertEquals(
            forward.stream().map(HashSet::new).toList(),
            backward.stream().map(HashSet::new).toList());
    }

    private static List<List<SegmentHeader>> candidates(SegmentIdentity target, SegmentHeader... headers) {
        final SegmentCacheIndexImpl index = new SegmentCacheIndexImpl(Thread.currentThread());
        for (SegmentHeader header : headers) {
            index.add(header, false);
        }
        return index.findRollupCandidates(target, Map.of("[f].[a]", "v"));
    }

    /** Region over the given columns appended to the header. */
    private static SegmentHeader withRegion(SegmentHeader header, SegmentColumn... box) {
        return header.constrain(box);
    }

    private static SegmentColumn col(String expression, String... values) {
        return new SegmentColumn(expression, 10,
            new TreeSet<>(List.of(values)));
    }

    @Test
    void regionMissingTheRequestKeepsTheHeaderARollupCandidate() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        final SegmentHeader header =
            withRegion(header("x"), col("[f].[a]", "other"));
        index.add(header, false);

        BitKey coarse = BitKey.Factory.makeBitKey(3);
        coarse.set(0);
        SegmentIdentity target = new SegmentIdentity("Schema", CHECKSUM, "Cube",
            "FACT", "m1", List.of(), coarse);
        List<List<SegmentHeader>> candidates =
            index.findRollupCandidates(target, Map.of("[f].[a]", "x"));
        assertEquals(1, candidates.size());
        assertTrue(candidates.get(0).contains(header));
    }

    @Test
    void regionOnTheRequestedCoordinateExcludesTheHeader() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        index.add(withRegion(header("x"), col("[f].[a]", "x")), false);

        BitKey coarse = BitKey.Factory.makeBitKey(3);
        coarse.set(0);
        SegmentIdentity target = new SegmentIdentity("Schema", CHECKSUM, "Cube",
            "FACT", "m1", List.of(), coarse);
        assertTrue(index.findRollupCandidates(target, Map.of("[f].[a]", "x")).isEmpty());
    }

    @Test
    void regionOnlyOverAggregatedAwayColumnsExcludesTheHeader() {
        // widened to the kept columns the box loses every constraint and
        // would taint the whole target
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        index.add(withRegion(header("x"), col("[f].[b]", "q")), false);

        BitKey coarse = BitKey.Factory.makeBitKey(3);
        coarse.set(0);
        SegmentIdentity target = new SegmentIdentity("Schema", CHECKSUM, "Cube",
            "FACT", "m1", List.of(), coarse);
        assertTrue(index.findRollupCandidates(target, Map.of("[f].[a]", "x")).isEmpty());
    }

    @Test
    void keptColumnMissingTheRequestNeutralizesAggregatedAwayRegionColumns() {
        // box (a=other, b=q): widened to (a=other), which misses the request
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        final SegmentHeader header = withRegion(header("x"),
            col("[f].[a]", "other"), col("[f].[b]", "q"));
        index.add(header, false);

        BitKey coarse = BitKey.Factory.makeBitKey(3);
        coarse.set(0);
        SegmentIdentity target = new SegmentIdentity("Schema", CHECKSUM, "Cube",
            "FACT", "m1", List.of(), coarse);
        List<List<SegmentHeader>> candidates =
            index.findRollupCandidates(target, Map.of("[f].[a]", "x"));
        assertEquals(1, candidates.size());
        assertTrue(candidates.get(0).contains(header));
    }

    /** Header over only [f].[a] (bit 0) — the request's own dimensionality. */
    private static SegmentHeader headerOverA(String aValue) {
        BitKey bitKey = BitKey.Factory.makeBitKey(3);
        bitKey.set(0);
        return new SegmentHeader("Schema", CHECKSUM, "Cube", "m1",
            List.of(new SegmentColumn("[f].[a]", 10,
                new TreeSet<>(List.of(aValue)))),
            List.of(), "FACT", bitKey, List.of());
    }

    @Test
    void rollupAncestorsAreStrictSupersetsFewestBitsFirst() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        final SegmentHeader wide = headerABC("x", null, null);   // bits 0,1,2
        final SegmentHeader mid = header("x");                   // bits 0,1
        final SegmentHeader exact = headerOverA("x");            // bit 0
        index.add(wide, false);
        index.add(mid, false);
        index.add(exact, false);

        BitKey coarse = BitKey.Factory.makeBitKey(3);
        coarse.set(0);
        SegmentIdentity target = new SegmentIdentity("Schema", CHECKSUM, "Cube",
            "FACT", "m1", List.of(), coarse);
        index.findRollupCandidates(target, Map.of("[f].[a]", "x"));

        List<BitKey> ancestors =
            index.ancestorMemoForTests(mid).get(coarse.freeze());
        // strict supersets only (the request's own dimensionality is served
        // by locate, never by rollup), cheapest rollups first
        assertEquals(2, ancestors.size());
        assertEquals(2, ancestors.get(0).cardinality());
        assertEquals(3, ancestors.get(1).cardinality());
        assertFalse(ancestors.contains(coarse));
    }

	@Test
    void noHeaderOnLoad() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());

        final SegmentHeader header = mock(SegmentHeader.class);
        final SegmentBody body = mock(SegmentBody.class);

        // This should not fail.
        index.loadSucceeded(header, body);
        // late data for an unknown header is discarded
        assertFalse(index.contains(header));
    }

    /**
     * loadFailed for a header that is registered but NOT loading (a
     * rollup already completed its slot, the SQL load fails later) is a
     * no-op: throwing on the actor skipped nothing useful and the loaded
     * body stays served. Red while it threw IllegalArgumentException.
     */
    @org.junit.jupiter.api.Test
    void loadFailedOnACompletedHeaderIsANoOp() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread(), command -> { });
        final SegmentHeader h = header("x");
        index.add(h, false); // registered, no load slot

        index.loadFailed(h, new RuntimeException("late SQL failure"));

        assertTrue(index.contains(h), "the completed header stays registered");
    }

    /**
     * A flush that hits a still-loading header only FLAGS it
     * (removeAfterLoad) - contains() stays true until the load completes.
     * isRegistered() is the probe for "does this load still have an
     * interested party": red while the store-put gate and the loader's
     * abort check used contains() and leaked the pre-flush body back into
     * the stores.
     */
    @org.junit.jupiter.api.Test
    void removeOnALoadingHeaderFlagsAndUnregisters() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread(), command -> { });
        final SegmentHeader h = header("x");

        index.add(h, true);
        assertTrue(index.contains(h));
        assertTrue(index.isRegistered(h));

        index.remove(h); // load pending: deferred removal, flag only

        assertTrue(index.contains(h),
            "the entry survives until the load completes");
        assertFalse(index.isRegistered(h),
            "but nobody is interested in it any more");
    }

    /**
     * Statement.cancel is a JDBC network round-trip; cancel(Execution)
     * runs on the actor, so the cancel itself is handed to the executor
     * instead of stalling the one actor thread on a dead database.
     */
    @org.junit.jupiter.api.Test
    void statementCancelRunsOnTheCancelExecutor() throws Exception {
        final List<Runnable> submitted = new ArrayList<>();
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread(), submitted::add);
        final SegmentHeader h = header("x");
        final org.eclipse.daanse.olap.api.execution.Execution exec =
            mock(org.eclipse.daanse.olap.api.execution.Execution.class);
        final java.sql.Statement stmt = mock(java.sql.Statement.class);
        final org.eclipse.daanse.olap.api.execution.GuardedStatement guard =
            new org.eclipse.daanse.olap.api.execution.GuardedStatement(stmt);

        index.add(h, true);
        index.getFuture(exec, h);
        index.linkSqlStatement(h, guard);
        index.cancel(exec);

        assertEquals(1, submitted.size());
        org.mockito.Mockito.verify(stmt, org.mockito.Mockito.never()).cancel();
        submitted.get(0).run();
        org.mockito.Mockito.verify(stmt).cancel();
    }

    /**
     * A load that registered but whose SQL thread has not linked its
     * statement yet cancels without a statement task - the raw null went
     * to the executor before and died inside the crush-all cancel.
     */
    @org.junit.jupiter.api.Test
    void cancelBeforeStatementLinkSubmitsNoTask() {
        final List<Runnable> submitted = new ArrayList<>();
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread(), submitted::add);
        final SegmentHeader h = header("x");
        final org.eclipse.daanse.olap.api.execution.Execution exec =
            mock(org.eclipse.daanse.olap.api.execution.Execution.class);

        index.add(h, true);
        index.getFuture(exec, h);
        index.cancel(exec);

        assertEquals(0, submitted.size());
    }

    /**
     * Mirror of the locate filter: a header flagged for removal after its
     * load (flushed while loading) must not seed a rollup - its store
     * body still exists in the flush window, and the rollup would publish
     * the flushed cells permanently.
     */
    @org.junit.jupiter.api.Test
    void rollupCandidatesSkipHeadersFlaggedForRemoval() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        final SegmentHeader fine = header("x");
        index.add(fine, true);

        BitKey coarse = BitKey.Factory.makeBitKey(3);
        coarse.set(0);
        final SegmentIdentity target = new SegmentIdentity("Schema", CHECKSUM, "Cube",
            "FACT", "m1", List.of(), coarse);
        // while loading it is no candidate either (open-slot filter)
        assertTrue(index.findRollupCandidates(target, Map.of("[f].[a]", "x")).isEmpty());

        index.remove(fine); // during load: flagged, not yet gone
        assertTrue(index.findRollupCandidates(target, Map.of("[f].[a]", "x")).isEmpty());

        // the deferred removal executes on load completion - still no candidate
        index.loadSucceeded(fine, mock(SegmentBody.class));
        assertTrue(index.findRollupCandidates(target, Map.of("[f].[a]", "x")).isEmpty());
        assertFalse(index.contains(fine));
    }

    /** update() of an unknown source must not seed the side maps with a ghost. */
    @org.junit.jupiter.api.Test
    void updateOfUnknownHeaderLeavesNoGhost() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        final SegmentHeader unknown = header("x");
        final SegmentHeader target = header("y");

        index.update(unknown, target);

        assertTrue(index.locate(target.identity(), Map.of("[f].[a]", "y")).isEmpty());
        assertFalse(index.contains(target));
    }

    /**
     * update() displacing a target header that is still loading must fail
     * the displaced slot - its waiters would otherwise hang until their
     * query timeout.
     */
    @org.junit.jupiter.api.Test
    void updateFailsTheDisplacedLoadingSlot() throws Exception {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        final SegmentHeader source = header("x");
        final SegmentHeader target = header("y");
        index.add(source, false);
        index.add(target, true);
        final org.eclipse.daanse.olap.api.execution.Execution exec =
            mock(org.eclipse.daanse.olap.api.execution.Execution.class);
        final Future<SegmentBody> waiter = index.getFuture(exec, target);

        index.update(source, target);

        assertThrows(ExecutionException.class,
            () -> waiter.get(5, TimeUnit.SECONDS));
    }

    /**
     * A flush that constrains a segment which is STILL LOADING must fail
     * the load slot: update() used to move the HeaderInfo (open slot
     * included) under the new key, loadSucceeded(oldHeader) then ran into
     * "data arrived late" and the waiters hung forever. Red before N1.
     */
    @org.junit.jupiter.api.Test
    void updateOfLoadingHeaderFailsItsWaiters() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        final SegmentHeader loading = header("x");
        final SegmentHeader constrained = header("x-constrained");
        index.add(loading, true);
        final org.eclipse.daanse.olap.api.execution.Execution exec =
            mock(org.eclipse.daanse.olap.api.execution.Execution.class);
        final Future<SegmentBody> waiter = index.getFuture(exec, loading);

        index.update(loading, constrained);

        assertThrows(ExecutionException.class,
            () -> waiter.get(5, TimeUnit.SECONDS));
        // the late load result is discarded, never adopted
        index.loadSucceeded(loading, mock(SegmentBody.class));
        assertFalse(index.contains(loading));
    }

    /**
     * A header whose load is still open must never seed a rollup: the
     * candidate's body is nowhere yet, the reader's miss would remove the
     * loading segment (store-without-index ghost + lost work). Red before N2.
     */
    @org.junit.jupiter.api.Test
    void rollupCandidatesSkipHeadersStillLoading() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        final SegmentHeader fine = header("x");
        index.add(fine, true);
        final org.eclipse.daanse.olap.api.execution.Execution exec =
            mock(org.eclipse.daanse.olap.api.execution.Execution.class);
        index.getFuture(exec, fine);

        BitKey coarse = BitKey.Factory.makeBitKey(3);
        coarse.set(0);
        final SegmentIdentity target = new SegmentIdentity("Schema", CHECKSUM, "Cube",
            "FACT", "m1", List.of(), coarse);
        assertTrue(index.findRollupCandidates(target, Map.of("[f].[a]", "x")).isEmpty());

        // once loaded, the header becomes a candidate
        index.loadSucceeded(fine, mock(SegmentBody.class));
        assertEquals(1, index.findRollupCandidates(target, Map.of("[f].[a]", "x")).size());
    }

    /**
     * A flush that hits a still-loading header must not abandon the load while
     * peers are parked on its slot: they are handed the body first and only
     * then is the header evicted. isRegistered says "do not put this in the
     * stores"; hasInterestedParties says "somebody is still owed a result".
     */
    @Test
    void flushedHeaderKeepsItsInterestedParties() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        final SegmentHeader header = header("x");
        index.add(header, true);

        // a peer parks on the pending slot
        final Execution peer = mock(Execution.class);
        assertNotNull(index.getFuture(peer, header), "peer should get the pending slot");

        // an administrative flush lands mid-load
        index.remove(header);

        assertFalse(index.isRegistered(header),
            "a flushed header must stay out of the external stores");
        assertTrue(index.hasInterestedParties(header),
            "the parked peer is still owed the body it is waiting for");
    }

    /** Without a waiting peer the flushed load is nobody's business and may stop. */
    @Test
    void flushedHeaderWithoutClientsHasNoInterestedParties() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        final SegmentHeader header = header("x");
        index.add(header, true);

        index.remove(header);

        assertFalse(index.isRegistered(header));
        assertFalse(index.hasInterestedParties(header),
            "nobody waits, so the SQL should not burn on to feed a ghost");
    }

    /** An unknown header has nobody waiting either. */
    @Test
    void unknownHeaderHasNoInterestedParties() {
        final SegmentCacheIndexImpl index =
            new SegmentCacheIndexImpl(Thread.currentThread());
        assertFalse(index.hasInterestedParties(header("x")));
    }
}
