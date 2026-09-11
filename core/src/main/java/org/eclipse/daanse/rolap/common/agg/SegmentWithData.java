/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
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

import java.util.List;

import org.eclipse.daanse.olap.api.result.NullValue;
import org.eclipse.daanse.olap.key.BitKey;
import org.eclipse.daanse.olap.key.CellKey;
import org.eclipse.daanse.rolap.common.star.RolapStar;
import org.eclipse.daanse.rolap.common.star.StarColumnPredicate;
import org.eclipse.daanse.rolap.common.star.StarPredicate;

/**
 * Extension to {@link Segment} with a data set.
 *
 * @author jhyde
 */
public class SegmentWithData extends Segment {
    /**
     * An array of axes, one for each constraining column, containing the values
     * returned for that constraining column.
 */
    final SegmentAxis[] axes;

    /**
     * The cell values of this segment. Final and set in the constructor -
     * a SegmentWithData is only ever built from a COMPLETE dataset (the
     * loading state lives in the index's slots, not here), so reads need
     * no guard; neither the reference nor the contents change afterwards.
     */
    private final SegmentDataset data;

    /**
     * Creates a SegmentWithData from an existing Segment.
     *
     * @param segment Segment (without data)
     * @param data Data set
 */
    public SegmentWithData(
        Segment segment,
        SegmentDataset data,
        SegmentAxis[] axes)
    {
        this(
            segment.getStar(),
            segment.getConstrainedColumnsBitKey(),
            segment.getColumns(),
            segment.measure,
            segment.predicates,
            segment.getExcludedRegions(),
            segment.compoundPredicateList,
            data,
            axes);
        if (segment instanceof SegmentWithData) {
            throw new AssertionError();
        }
    }

    /**
     * Creates a SegmentWithData.
     *
     * @param star Star that this Segment belongs to
     * @param measure Measure whose values this Segment contains
     * @param predicates List of axes; each is a constraint plus a list of
     *     values.
     * @param excludedRegions List of regions which are not in this segment.
 */
    private SegmentWithData(
        RolapStar star,
        BitKey constrainedColumnsBitKey,
        RolapStar.Column[] columns,
        RolapStar.Measure measure,
        StarColumnPredicate[] predicates,
        List<ExcludedRegion> excludedRegions,
        final List<StarPredicate> compoundPredicateList,
        SegmentDataset data,
        SegmentAxis[] axes)
    {
        super(
            star,
            constrainedColumnsBitKey,
            columns,
            measure,
            predicates,
            excludedRegions,
            compoundPredicateList);
        this.axes = axes;
        this.data = data;
    }

    @Override
    protected void describeAxes(StringBuilder buf, int i, boolean values) {
        super.describeAxes(buf, i, values);
        if (!values) {
            return;
        }
        Object[] keys = axes[i].getKeys();
        buf.append(", values={");
        for (int j = 0; j < keys.length; j++) {
            if (j > 0) {
                buf.append(", ");
            }
            Object key = keys[j];
            buf.append(key);
        }
        buf.append("}");
    }

    /**
     * Retrieves the value at the location identified by
     * keys.
     *
     * Returns
     *
     * {@link org.eclipse.daanse.olap.api.result.NullValue#INSTANCE} if the
     * cell value is null (because no fact table rows met those criteria) or
     * a NaN aggregate;
     *
     * null if the value is not supposed to be in this segment
     * (because one or more of the keys do not pass the axis criteria);
     *
     * the data value otherwise
 */
    public Object getCellValue(Object[] keys) {
        assert keys.length == axes.length;
        int missed = 0;
        CellKey cellKey = CellKey.Generator.newCellKey(axes.length);
        for (int i = 0; i < keys.length; i++) {
            Comparable key = (Comparable) keys[i];
            int offset = axes[i].getOffset(key);
            if (offset < 0) {
                if (axes[i].wouldContain(key)) {
                    // see whether this segment should contain this value
                    missed++;
                    continue;
                } else {
                    // this value should not appear in this segment; we
                    // should be looking in a different segment
                    return null;
                }
            }
            cellKey.setAxis(i, offset);
        }
        if (isExcluded(keys)) {
            // this value should not appear in this segment; we
            // should be looking in a different segment
            return null;
        }
        if (missed > 0) {
            // the value should be in this segment, but isn't, because one
            // or more of its keys does have any values
            return NullValue.INSTANCE;
        } else {
            Object o = data.getObject(cellKey);
            if (o == null) {
                o = NullValue.INSTANCE;
            } else if (o instanceof Double d && Double.isNaN(d)) {
                // NaN aggregate = undefined 0/0 (e.g. avg over zero facts).
                // Present as empty, matching NULL from division-by-zero.
                o = NullValue.INSTANCE;
            }
            return o;
        }
    }

    /**
     * Returns the number of cells in this Segment, deducting cells in
     * excluded regions.
     *
     * This method may return a value which is slightly too low, or
     * occasionally even negative. This occurs when a Segment has more than one
     * excluded region, and those regions overlap. Cells which are in both
     * regions will be counted twice.
     *
     * @return Number of cells in this Segment
 */
    public int getCellCount() {
        int cellCount = 1;
        for (SegmentAxis axis : axes) {
            cellCount *= axis.getKeys().length;
        }
        for (ExcludedRegion excludedRegion : excludedRegions) {
            cellCount -= excludedRegion.getCellCount();
        }
        return cellCount;
    }

    
    /**
     * Returns the data set.
     *
     * WARNING: the returned SegmentDataset reference should not be modified;
     * it is assumed to be invariant.
     *
     * @return The data reference
 */
    /** The segment's axes, in coordinate order. Callers must not mutate. */
    public SegmentAxis[] getAxes() {
        return axes.clone();
    }

    public final SegmentDataset getData() {
        return data;
    }
}
