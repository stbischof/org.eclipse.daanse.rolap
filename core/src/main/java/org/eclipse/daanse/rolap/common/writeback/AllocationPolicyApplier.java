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
 *   SmartCity Jena - initial
 *   Stefan Bischof (bipolis.org) - initial
 */
package org.eclipse.daanse.rolap.common.writeback;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.daanse.olap.api.DataTypeJdbc;
import org.eclipse.daanse.olap.api.element.Member;
import org.eclipse.daanse.olap.api.result.AllocationPolicy;

/**
 * Turns "set this aggregate to {@code value}" into per-cell delta rows:
 * for every target cell one row cancelling the old value and one row
 * carrying the new share.
 *
 * Cell values arrive as whatever the evaluator produced - Double,
 * Integer, BigDecimal or null (empty cell) - and are normalized through
 * {@link #num(Object)}; a hard {@code (Double)} cast used to NPE on the
 * first empty target cell. A zero base sum degrades the WEIGHTED
 * policies to their EQUAL siblings instead of dividing by zero (a fresh
 * plan starts at 0 everywhere - NaN rows reached the writeback table).
 * Integer-typed measure columns get largest-remainder rounding over the
 * shares, so the rounded rows still sum to the rounded target.
 */
public final class AllocationPolicyApplier {

    private AllocationPolicyApplier() {
    }

    public static List<Map<String, Map.Entry<DataTypeJdbc, Object>>> allocateData(
            Map<List<Member>, Object> data,
            String measureName,
            Double value,
            AllocationPolicy allocation,
            RolapWritebackTable writebackTable
        ) {
            if (data.isEmpty()) {
                throw new IllegalArgumentException(
                    "writeback allocation found no target cells for " + measureName);
            }
            Map<List<Member>, Double> d = new HashMap<>();
            Map<List<Member>, Double> dMinus = new HashMap<>();
            int size = data.size();
            double target = num(value);
            double sum = data.values().stream().mapToDouble(AllocationPolicyApplier::num).sum();

            AllocationPolicy effective = allocation;
            if (sum == 0d && allocation == AllocationPolicy.WEIGHTED_ALLOCATION) {
                effective = AllocationPolicy.EQUAL_ALLOCATION;
            } else if (sum == 0d && allocation == AllocationPolicy.WEIGHTED_INCREMENT) {
                effective = AllocationPolicy.EQUAL_INCREMENT;
            }

            switch (effective) {
                case WEIGHTED_ALLOCATION:
                    for (Map.Entry<List<Member>, Object> entry : data.entrySet()) {
                        double old = num(entry.getValue());
                        dMinus.put(entry.getKey(), -old);
                        d.put(entry.getKey(), target / sum * old);
                    }
                    break;
                case EQUAL_INCREMENT:
                    double offset = target - sum;
                    for (Map.Entry<List<Member>, Object> entry : data.entrySet()) {
                        double old = num(entry.getValue());
                        dMinus.put(entry.getKey(), -old);
                        d.put(entry.getKey(), old + offset / size);
                    }
                    break;
                case WEIGHTED_INCREMENT:
                    offset = target - sum;
                    for (Map.Entry<List<Member>, Object> entry : data.entrySet()) {
                        double old = num(entry.getValue());
                        dMinus.put(entry.getKey(), -old);
                        d.put(entry.getKey(), old + offset / sum * old);
                    }
                    break;
                case EQUAL_ALLOCATION:
                default:
                    double val = target / size;
                    for (Map.Entry<List<Member>, Object> entry : data.entrySet()) {
                        dMinus.put(entry.getKey(), -num(entry.getValue()));
                        d.put(entry.getKey(), val);
                    }
            }

            if (isIntegerColumn(measureName, writebackTable)) {
                // per-row rounding drifted: round(-0.5)+round(3.33) per cell
                // summed to 9 where the target was 10. The old-value side is
                // rounded per cell (integral in an integer fact anyway); the
                // shares get largest-remainder so their sum is the target.
                dMinus.replaceAll((k, v) -> (double) Math.round(v));
                largestRemainder(d, Math.round(target));
            }

            List<Map<List<Member>, Double>> res = new ArrayList<>();
            res.add(dMinus);
            res.add(d);
            return WritebackRowBuilder.allocateData(res, measureName, writebackTable);
        }

    /** Evaluator output is Double, Integer, BigDecimal or null (empty cell). */
    static double num(Object value) {
        return value instanceof Number number ? number.doubleValue() : 0d;
    }

    private static boolean isIntegerColumn(String measureName, RolapWritebackTable writebackTable) {
        for (RolapWritebackColumn column : writebackTable.getColumns()) {
            if (column instanceof RolapWritebackMeasure measure
                    && measure.getMeasure().getUniqueName().equals(measureName)) {
                DataTypeJdbc type = measure.getColumn().getType();
                return DataTypeJdbc.INTEGER.equals(type) || DataTypeJdbc.BIGINT.equals(type);
            }
        }
        return false;
    }

    /**
     * Floors every share and hands the missing units to the largest
     * fractional remainders, so the rounded shares sum exactly to
     * {@code targetTotal}. Mathematically sum(d) == target for every
     * policy above, which makes the remainder count 0..size-1.
     */
    private static void largestRemainder(Map<List<Member>, Double> d, long targetTotal) {
        List<Map.Entry<List<Member>, Double>> entries = new ArrayList<>(d.entrySet());
        long floorSum = 0;
        double[] remainders = new double[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            double share = entries.get(i).getValue();
            long floor = (long) Math.floor(share);
            remainders[i] = share - floor;
            d.put(entries.get(i).getKey(), (double) floor);
            floorSum += floor;
        }
        long missing = targetTotal - floorSum;
        // hand out one unit per pass to the largest remainder not yet topped up
        boolean[] used = new boolean[entries.size()];
        while (missing > 0) {
            int best = -1;
            for (int i = 0; i < entries.size(); i++) {
                if (!used[i] && (best < 0 || remainders[i] > remainders[best])) {
                    best = i;
                }
            }
            if (best < 0) {
                break; // more units than cells: numeric edge, leave the rest
            }
            used[best] = true;
            d.put(entries.get(best).getKey(), d.get(entries.get(best).getKey()) + 1d);
            missing--;
        }
    }
}
