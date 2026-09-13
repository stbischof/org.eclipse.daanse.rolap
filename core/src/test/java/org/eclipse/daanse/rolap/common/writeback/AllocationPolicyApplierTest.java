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
package org.eclipse.daanse.rolap.common.writeback;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.daanse.olap.api.DataTypeJdbc;
import org.eclipse.daanse.olap.api.element.Member;
import org.eclipse.daanse.olap.api.result.AllocationPolicy;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The allocation math against real evaluator output: cell values arrive
 * as Double, Integer, BigDecimal or null; a fresh plan has base sum 0;
 * integer target columns must not drift under per-row rounding. All
 * four cases produced NPE, ClassCastException, NaN rows or off-by-one
 * totals before.
 */
class AllocationPolicyApplierTest {

    private static final String MEASURE = "[Measures].[AmountPlan]";

    private final RolapWritebackTable table = mock(RolapWritebackTable.class);

    private void tableWithMeasureColumn(DataTypeJdbc type) {
        RolapWritebackMeasure column =
            mock(RolapWritebackMeasure.class, Mockito.RETURNS_DEEP_STUBS);
        when(column.getMeasure().getUniqueName()).thenReturn(MEASURE);
        when(column.getColumn().getName()).thenReturn("AMOUNT");
        when(column.getColumn().getType()).thenReturn(type);
        when(table.getColumns()).thenReturn(List.of(column));
    }

    private static Map<List<Member>, Object> cells(Object... values) {
        Map<List<Member>, Object> data = new LinkedHashMap<>();
        for (Object value : values) {
            data.put(List.of(mock(Member.class)), value);
        }
        return data;
    }

    private static double total(List<Map<String, Map.Entry<DataTypeJdbc, Object>>> rows) {
        return rows.stream()
            .mapToDouble(r -> ((Number) r.get("AMOUNT").getValue()).doubleValue())
            .sum();
    }

    /** Cell values of any numeric type, and null, are handled. */
    @Test
    void mixedValueTypesAndNullsAllocate()
    {
        tableWithMeasureColumn(DataTypeJdbc.NUMERIC);
        var rows = AllocationPolicyApplier.allocateData(
            cells(null, 2, new BigDecimal("3.5")), MEASURE, 12.0,
            AllocationPolicy.EQUAL_ALLOCATION, table);

        // -(0 + 2 + 3.5) cancelled, +12 allocated
        assertThat(total(rows)).isCloseTo(12.0 - 5.5, org.assertj.core.data.Offset.offset(1e-9));
    }

    /** A zero-weight denominator produces no NaN rows in the writeback table. */
    @Test
    void zeroBaseSumFallsBackToEqualAllocation() {
        tableWithMeasureColumn(DataTypeJdbc.NUMERIC);
        var rows = AllocationPolicyApplier.allocateData(
            cells(0d, 0d, 0d), MEASURE, 9.0,
            AllocationPolicy.WEIGHTED_ALLOCATION, table);

        assertThat(rows).allSatisfy(row -> {
            double v = ((Number) row.get("AMOUNT").getValue()).doubleValue();
            assertThat(Double.isFinite(v)).as("no NaN/Infinity rows").isTrue();
        });
        assertThat(total(rows)).isCloseTo(9.0, org.assertj.core.data.Offset.offset(1e-9));
    }

    @Test
    void zeroBaseSumFallsBackToEqualIncrement() {
        tableWithMeasureColumn(DataTypeJdbc.NUMERIC);
        var rows = AllocationPolicyApplier.allocateData(
            cells(0d, 0d), MEASURE, 5.0,
            AllocationPolicy.WEIGHTED_INCREMENT, table);

        assertThat(total(rows)).isCloseTo(5.0, org.assertj.core.data.Offset.offset(1e-9));
    }

    /** Rounded per-row allocations sum to the requested total. */
    @Test
    void integerColumnsRoundWithoutDrift() {
        tableWithMeasureColumn(DataTypeJdbc.INTEGER);
        var rows = AllocationPolicyApplier.allocateData(
            cells(0d, 0d, 0d), MEASURE, 10.0,
            AllocationPolicy.EQUAL_ALLOCATION, table);

        assertThat(total(rows)).isEqualTo(10.0);
        assertThat(rows).allSatisfy(row -> {
            double v = ((Number) row.get("AMOUNT").getValue()).doubleValue();
            assertThat(v % 1).as("integral rows only").isZero();
        });
    }

    @Test
    void emptyTargetSetIsRefusedWithAClearMessage() {
        tableWithMeasureColumn(DataTypeJdbc.NUMERIC);
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> AllocationPolicyApplier.allocateData(
                Map.of(), MEASURE, 1.0, AllocationPolicy.EQUAL_ALLOCATION, table));
    }
}
