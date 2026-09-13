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
package org.eclipse.daanse.rolap.common.nativize;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.eclipse.daanse.olap.api.calc.tuple.TupleList;
import org.eclipse.daanse.olap.calc.base.type.tuplebase.TupleCollections;
import org.eclipse.daanse.rolap.common.nativize.RolapNativeSet.TupleCacheKey;
import org.junit.jupiter.api.Test;

/**
 * Native tuple cache flush semantics after the epoch simplification:
 * every flush clears the whole cache (no per-entry hierarchy tags any
 * more), and key identity lives in the parts alone. The put-then-validate
 * epoch handshake in executeList is covered end-to-end by the testkit
 * native activation probes.
 */
class RolapNativeSetFlushTest {

    private static final class TestSet extends RolapNativeSet {
        TestSet() {
            super(1000);
        }

        @Override
        protected boolean restrictMemberTypes() {
            return false;
        }

        @Override
        org.eclipse.daanse.olap.api.evaluator.NativeEvaluator createEvaluator(
                org.eclipse.daanse.rolap.common.evaluator.RolapEvaluator evaluator,
                org.eclipse.daanse.olap.api.function.FunctionDefinition fun,
                org.eclipse.daanse.olap.api.query.component.Expression[] args,
                boolean enableNativeFilter) {
            return null;
        }
    }

    private static TupleList emptyTuples() {
        return TupleCollections.createList(1);
    }

    @Test
    void flushClearsEveryEntry() {
        TestSet set = new TestSet();
        TupleCacheKey first = new TupleCacheKey(List.of("a"));
        TupleCacheKey second = new TupleCacheKey(List.of("b"));
        set.testCache().put(first, emptyTuples());
        set.testCache().put(second, emptyTuples());

        set.flushCache();

        assertThat(set.testCache().get(first)).isNull();
        assertThat(set.testCache().get(second)).isNull();
    }

    @Test
    void keyIdentityLivesInThePartsAlone() {
        TupleCacheKey key1 = new TupleCacheKey(List.of("same"));
        TupleCacheKey key2 = new TupleCacheKey(List.of("same"));
        assertThat(key1).isEqualTo(key2).hasSameHashCodeAs(key2);
        assertThat(key1).isNotEqualTo(new TupleCacheKey(List.of("other")));
    }
}
