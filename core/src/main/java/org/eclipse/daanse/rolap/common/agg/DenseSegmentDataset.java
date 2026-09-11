/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2002-2005 Julian Hyde
 * Copyright (C) 2005-2017 Hitachi Vantara and others
 * All Rights Reserved.
 *
 * jhyde, 21 March, 2002
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


import org.eclipse.daanse.olap.key.CellKey;

/**
 * A DenseSegmentDataset is a means of storing segment values
 * which is suitable when most of the combinations of keys have a value
 * present.
 *
 * The storage requirements are as follows. Table requires 1 word per
 * cell.
 *
 * @author jhyde
 * @since 21 March, 2002
 */
abstract class DenseSegmentDataset implements SegmentDataset {
    private final SegmentAxis[] axes;
    protected final int[] axisMultipliers;

    /**
     * Creates a DenseSegmentDataset.
     *
     * @param axes Segment axes, containing actual column values
     */
    DenseSegmentDataset(SegmentAxis[] axes) {
        this.axes = axes;
        this.axisMultipliers = computeAxisMultipliers();
    }

    private int[] computeAxisMultipliers() {
        final int[] axisMultipliersInner = new int[axes.length];
        int multiplier = 1;
        for (int i = axes.length - 1; i >= 0; --i) {
            final SegmentAxis axis = axes[i];
            axisMultipliersInner[i] = multiplier;
            multiplier *= axis.getKeys().length;
        }
        return axisMultipliersInner;
    }

    protected abstract Object getObject(int i);

    protected final int getOffset(int[] keys) {
        return CellKey.Generator.getOffset(keys, axisMultipliers);
    }


    @Override
	public Object getObject(CellKey pos) {
        throw new UnsupportedOperationException();
    }

    @Override
	public int getInt(CellKey pos) {
        throw new UnsupportedOperationException();
    }

    @Override
	public double getDouble(CellKey pos) {
        throw new UnsupportedOperationException();
    }

    protected abstract int getSize();

}
