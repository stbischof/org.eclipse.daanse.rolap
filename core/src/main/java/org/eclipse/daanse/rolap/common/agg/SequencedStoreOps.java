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
package org.eclipse.daanse.rolap.common.agg;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.eclipse.daanse.olap.util.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-segment-id ordering of store operations on one executor: an
 * operation submitted under an id runs strictly after every earlier
 * operation under the same id (a flush issued after a put removes what
 * the put stored, never loses the race). A chain entry cleans itself up
 * once it drains, so an idle instance holds no memory per id.
 *
 * Each {@link SegmentCacheManager} owns ONE instance; the session
 * overlay shares the manager's executor but chains on its own map (its
 * store set is disjoint - the overlay's in-memory store only).
 */
final class SequencedStoreOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(SequencedStoreOps.class);

    /** Chain tail per segment id; absent = no operation in flight. */
    private final ConcurrentHashMap<ByteString, CompletableFuture<?>> chains =
        new ConcurrentHashMap<>();

    // serializes the CLAIM phase of dual-id ops: pairwise-ordered claims
    // still deadlock over a RING of overlapping pairs; with claims globally
    // serialized, every wait edge points from a later to an earlier claimer
    private final Object dualClaimLock = new Object();

    private final Executor executor;

    SequencedStoreOps(Executor executor) {
        this.executor = executor;
    }

    /** Runs {@code op} on the executor, ordered with every other operation on {@code id}. */
    <T> CompletableFuture<T> sequenced(ByteString id, Supplier<T> op) {
        final CompletableFuture<T> result = new CompletableFuture<>();
        final CompletableFuture<?> prev = claimSlot(id, result);
        // release registered BEFORE the submit: a synchronous executor
        // rejection (shutdown) must not leave the slot claimed forever
        result.whenComplete((v, e) -> releaseSlot(id, result));
        chainProbe(result, prev, null, op);
        return result;
    }

    /**
     * Sequences an operation under BOTH segment ids: a rename reads under
     * the old id and writes under the new, so follow-ups on either id must
     * run after it. The claim phase is globally serialized (tiny critical
     * section, two map writes) - crossing renames then wait one on the
     * other, never on each other.
     */
    <T> CompletableFuture<T> sequenced(ByteString oldId, ByteString newId, Supplier<T> op) {
        if (oldId.equals(newId)) {
            return sequenced(oldId, op);
        }
        final CompletableFuture<T> result = new CompletableFuture<>();
        final CompletableFuture<?> prevFirst;
        final CompletableFuture<?> prevSecond;
        synchronized (dualClaimLock) {
            prevFirst = claimSlot(oldId, result);
            prevSecond = claimSlot(newId, result);
        }
        // release registered BEFORE the submit (synchronous rejection safety)
        result.whenComplete((value, error) -> {
            releaseSlot(oldId, result);
            releaseSlot(newId, result);
        });
        chainProbe(result, prevFirst, prevSecond, op);
        return result;
    }

    /**
     * Waits until every queued operation has run. Completed chains remove
     * themselves, so this drains to quiescence; new operations race in
     * from queries still running elsewhere.
     */
    void awaitQuiescence() {
        for (CompletableFuture<?> op : chains.values().toArray(CompletableFuture[]::new)) {
            try {
                op.get(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (TimeoutException e) {
                LOGGER.warn("pending cache write did not finish within 30s", e);
            } catch (Exception e) {
                // chain failures are logged where they occur
            }
        }
    }

    /** Replaces the id's chain tail with {@code next}; returns the previous tail, failure-absorbed. */
    private CompletableFuture<?> claimSlot(ByteString id, CompletableFuture<?> next) {
        final CompletableFuture<?>[] prev = new CompletableFuture<?>[1];
        chains.compute(id, (k, current) -> {
            prev[0] = current;
            return next;
        });
        return prev[0] == null
            ? CompletableFuture.completedFuture(null)
            : prev[0].exceptionally(e -> null);
    }

    private void releaseSlot(ByteString id, CompletableFuture<?> next) {
        chains.compute(id, (k, current) -> current == next ? null : current);
    }

    private <T> void chainProbe(CompletableFuture<T> result, CompletableFuture<?> first,
            CompletableFuture<?> second, Supplier<T> op) {
        // The submit to the executor happens when the PREDECESSOR
        // completes - possibly later, on the completing thread. A rejection
        // there (executor shut down mid-race) used to escape that thread's
        // postComplete and, worse, leave `result` forever incomplete: its
        // releaseSlot never ran and the id's chain slot stayed claimed for
        // good. Every path now completes `result`.
        (second == null ? first : CompletableFuture.allOf(first, second))
            .whenComplete((ignored, ignoredError) -> {
                try {
                    executor.execute(() -> {
                        try {
                            result.complete(op.get());
                        } catch (Throwable t) {
                            result.completeExceptionally(t);
                        }
                    });
                } catch (RuntimeException rejected) {
                    result.completeExceptionally(rejected);
                }
            });
    }
}
