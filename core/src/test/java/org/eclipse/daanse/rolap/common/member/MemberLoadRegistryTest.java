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

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.daanse.olap.api.access.Role;
import org.eclipse.daanse.olap.api.element.Hierarchy;
import org.eclipse.daanse.olap.api.element.Level;
import org.eclipse.daanse.olap.api.execution.Execution;
import org.eclipse.daanse.olap.api.execution.ExecutionContext;
import org.eclipse.daanse.olap.api.execution.ExecutionMetadata;
import org.eclipse.daanse.rolap.element.RolapHierarchy;
import org.mockito.Mockito;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.eclipse.daanse.rolap.api.element.RolapMember;
import org.eclipse.daanse.rolap.common.member.MemberLoadRegistry.Claim;
import org.eclipse.daanse.rolap.common.member.MemberLoadRegistry.LevelKey;
import org.junit.jupiter.api.Test;

class MemberLoadRegistryTest {

    private final MemberLoadRegistry registry = new MemberLoadRegistry();
    private final Object key = new LevelKey("level", "constraint");

    @Test
    void firstClaimLoadsSecondAwaitsTheSameResult() throws Exception {
        Claim loader = registry.claim(key);
        assertThat(loader.loader()).isTrue();

        Claim waiter = registry.claim(key);
        assertThat(waiter.loader()).isFalse();
        assertThat(waiter.future()).isSameAs(loader.future());

        List<RolapMember> result = List.of();
        CountDownLatch awaiting = new CountDownLatch(1);
        CompletableFuture<List<RolapMember>> awaited = CompletableFuture.supplyAsync(() -> {
            awaiting.countDown();
            return registry.await(waiter);
        });
        assertThat(awaiting.await(5, TimeUnit.SECONDS)).isTrue();
        registry.complete(loader, result);
        assertThat(awaited.get(5, TimeUnit.SECONDS)).isSameAs(result);

        // the key is released: the next claim is a loader again
        assertThat(registry.claim(key).loader()).isTrue();
    }

    @Test
    void loaderFailureReachesTheWaiterAndReleasesTheKey() {
        Claim loader = registry.claim(key);
        Claim waiter = registry.claim(key);

        RuntimeException boom = new IllegalStateException("load failed");
        registry.fail(loader, boom);

        assertThatThrownBy(() -> registry.await(waiter)).isSameAs(boom);
        // no hang and no poisoning: the next claimant may retry the load
        assertThat(registry.claim(key).loader()).isTrue();
    }

    @Test
    void loadEntryMarksTheThreadAndNestsCorrectly() {
        assertThat(registry.inLoad()).isFalse();
        registry.enterLoad();
        assertThat(registry.inLoad()).isTrue();
        registry.enterLoad();
        registry.exitLoad();
        assertThat(registry.inLoad()).isTrue();
        registry.exitLoad();
        assertThat(registry.inLoad()).isFalse();
    }

    @Test
    void flushDuringLoadFencesTheCacheWrite() {
        // outside a load, writes are always allowed
        assertThat(registry.threadWritesAllowed()).isTrue();

        registry.enterLoad();
        assertThat(registry.threadWritesAllowed()).isTrue();
        registry.bumpGeneration(); // a flush intervenes
        assertThat(registry.threadWritesAllowed()).isFalse();
        registry.exitLoad();

        // the next load starts under the new generation and may write again
        registry.enterLoad();
        assertThat(registry.threadWritesAllowed()).isTrue();
        registry.exitLoad();
    }

    /**
     * A canceled execution leaves the wait on someone else's load within
     * the poll slice rather than sitting out the full cap: the await is
     * sliced and re-checks cancellation, so a waiter does not hold its
     * shepherd slot for a load it no longer wants.
     */
    @Test
    void awaitLeavesOnExecutionCancel() throws Exception {
        Claim loader = registry.claim(key);
        assertThat(loader.loader()).isTrue();
        Claim waiter = registry.claim(key);

        ExecutionContext ctx =
            ExecutionContext.root(
                Optional.empty(),
                ExecutionMetadata.of("t", "t", null, 0));
        AtomicReference<Throwable> thrown =
            new AtomicReference<>();
        Thread waiterThread = new Thread(() -> {
            try {
                ExecutionContext.where(
                    ctx, () -> registry.await(waiter));
            } catch (Throwable t) {
                thrown.set(t);
            }
        });
        waiterThread.start();
        Thread.sleep(200);
        ctx.cancel();
        waiterThread.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(waiterThread.isAlive())
            .as("the waiter must leave the await after the cancel")
            .isFalse();
        assertThat(thrown.get()).isNotNull();
        // let the loader finish cleanly for hygiene
        registry.complete(loader, List.of());
    }

    /**
     * The registry walk unwraps delegating chains: role-restricted (and
     * ragged) hierarchies wrap the caching reader in a
     * RestrictedMemberReader, and the fence has to reach the registry
     * through that wrapper to hold on a connection with a non-default
     * role.
     */
    @Test
    void registryWalkUnwrapsDelegatingChains() {
        RolapHierarchy hierarchy =
            Mockito.mock(RolapHierarchy.class,
                Mockito.RETURNS_DEEP_STUBS);
        Mockito.doReturn(true).when(hierarchy).isRagged();
        List<Level> levels =
            new ArrayList<>();
        levels.add(null);
        Mockito.doReturn(levels).when(hierarchy).getLevels();

        CachingMemberReader caching =
            Mockito.mock(CachingMemberReader.class);
        Mockito.when(caching.loadRegistry()).thenReturn(registry);
        Mockito.doReturn(hierarchy).when(caching).getHierarchy();

        assertThat(MemberLoadRegistry.of(caching)).isSameAs(registry);

        Role role =
            Mockito.mock(Role.class);
        Mockito.doReturn(null).when(role).getAccessDetails(
            Mockito.any(Hierarchy.class));
        MemberReader wrapped = new CachingRestrictedMemberReader(caching, role);
        assertThat(MemberLoadRegistry.of(wrapped)).isSameAs(registry);

        MemberReader nonCaching = Mockito.mock(MemberReader.class);
        assertThat(MemberLoadRegistry.of(nonCaching)).isNull();
    }

    // --- withMemberLoadFences bracket contract -------------------------

    @Test
    void fencedReadReportsWritesAllowedOnQuietGenerations() {
        MemberLoadRegistry other = new MemberLoadRegistry();
        MemberLoadRegistry.Fenced<String> fenced = MemberLoadRegistry
            .withMemberLoadFences(List.of(registry, other), () -> "rows");
        assertThat(fenced.value()).isEqualTo("rows");
        assertThat(fenced.writesAllowed()).isTrue();
        assertThat(registry.inLoad()).isFalse();
        assertThat(other.inLoad()).isFalse();
    }

    @Test
    void generationBumpDuringFencedReadForbidsPublishing() {
        MemberLoadRegistry other = new MemberLoadRegistry();
        MemberLoadRegistry.Fenced<String> fenced = MemberLoadRegistry
            .withMemberLoadFences(List.of(registry, other), () -> {
                other.bumpGeneration();
                return "stale";
            });
        // the read result itself stays valid - only cache publishing is off
        assertThat(fenced.value()).isEqualTo("stale");
        assertThat(fenced.writesAllowed()).isFalse();
        assertThat(registry.inLoad()).isFalse();
        assertThat(other.inLoad()).isFalse();
    }

    @Test
    void throwFromLaterEnterLoadExitsExactlyTheEnteredPrefix() {
        MemberLoadRegistry throwing = Mockito.spy(new MemberLoadRegistry());
        Mockito.doThrow(new IllegalStateException("boom"))
            .when(throwing).enterLoad();
        assertThatThrownBy(() -> MemberLoadRegistry.withMemberLoadFences(
                List.of(registry, throwing), () -> "unreached"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("boom");
        // the first registry was entered and must be exited again ...
        assertThat(registry.inLoad()).isFalse();
        // ... and ONLY the entered prefix: an extra exit would throw here
        assertThatThrownBy(registry::exitLoad)
            .isInstanceOf(IllegalStateException.class);
        assertThat(throwing.inLoad()).isFalse();
    }

    @Test
    void throwFromTheReadStillExitsEveryFence() {
        MemberLoadRegistry other = new MemberLoadRegistry();
        assertThatThrownBy(() -> MemberLoadRegistry.withMemberLoadFences(
                List.of(registry, other),
                () -> { throw new IllegalArgumentException("sql failed"); }))
            .isInstanceOf(IllegalArgumentException.class);
        assertThat(registry.inLoad()).isFalse();
        assertThat(other.inLoad()).isFalse();
    }
}
