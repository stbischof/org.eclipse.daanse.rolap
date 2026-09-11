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

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.eclipse.daanse.olap.key.BitKey;
import org.eclipse.daanse.rolap.common.star.RolapStar;
import org.eclipse.daanse.rolap.common.star.StarColumnPredicate;
import org.eclipse.daanse.olap.spi.SegmentIdentity;
import org.eclipse.daanse.olap.spi.SegmentPredicate;
import org.eclipse.daanse.rolap.common.star.StarPredicate;

/**
 * A CellRequest contains the context necessary to get a cell
 * value from a star.
 *
 * @author jhyde
 * @since 21 March, 2002
 */
public class CellRequest {
    private final RolapStar.Measure measure;
    public final boolean extendedContext;
    public final boolean drillThrough;

    // Constrained columns and their predicates: compact parallel arrays in
    // insertion order until check() co-sorts them by bit position.
    private RolapStar.Column[] columns = new RolapStar.Column[8];
    private StarColumnPredicate[] predicates = new StarColumnPredicate[8];

    /**
     * An array that contains the bit positions of each constrained
     * column.
     *
     * Used to allow us to convert from a numeric index (i.e., give me the
     * third constrained column) to the actual column as referenced
     * by bit position.
     *
     * For example, if the three constrained columns have bitPositions 3, 16,
     * and 21, the contents of this array should be [3,16,21].
     */
    private int[] columnBitPositions;

    /** Number of constrained columns; the used prefix of the arrays. */
    private int numColumns;

    /**
     * Reference back to the CellRequest's star.  All CellRequests in a
     * given query are associated with a single star. Keeping this
     * reference allows us to maintain a list of columns by bit position
     * in just one place (the star) rather than duplicate that
     * information in each CellRequest.
     */
    private RolapStar star = null;

    // Array of column values;
    // Not used to represent the compound members along one or more dimensions.
    private Object[] singleValues;

    /**
     * The sorted columns, created on the first read; a non-null value
     * freezes the request — {@link #addConstrainedColumn} throws then.
     */
    private RolapStar.Column[] columnsCache = null;

    /**
     * A bit is set for each column in the column list. Allows us to rapidly
     * figure out whether two requests are for the same column set.
     * These are all of the columns that are involved with a query, that is, all
     * required to be present in an aggregate table for the table be used to
     * fulfill the query.
     */
    // not final: the first read swaps the built key for its frozen form
    private BitKey constrainedColumnsBitKey;

    /**
     * Map from a compound key's column BitKey to the predicate defining
     * the compound member. Sorted by key for fast comparison with other
     * requests and existing segments; null until the first entry — most
     * requests have none.
     */
    private SortedMap<BitKey, StarPredicate> compoundPredicateMap = null;


    /** Wire form per compound key, filled when the caller already has it. */
    private SortedMap<BitKey, SegmentPredicate> compoundWireMap = null;

    /** Wire form of the compound predicates, derived lazily from the map. */
    private List<SegmentPredicate> compoundSegmentPredicates = null;

    private Map<String, Comparable> mappedCellValues;
    private List<StarPredicate> compoundPredicateList;
    private SegmentIdentity segmentIdentity;

    /** Set on the first bitkey read: the freeze point and the guard point coincide. */
    private boolean bitKeyPublished;


    /**
     * Whether the request is impossible to satisfy. This is set to 'true' if
     * contradictory constraints are applied to the same column. For example,
     * the levels [Customer].[City] and [Cities].[City] map to the same column
     * via the same join-path, and one constraint sets city = 'Burbank' and
     * another sets city = 'Los Angeles'.
     */
    private boolean unsatisfiable;

    /** True until check() co-sorts the arrays and freezes the request. */
    private boolean isDirty = true;

    /**
     * Creates a {@link CellRequest}.
     *
     * @param measure Measure the request is for
     * @param extendedContext If a drill-through request, whether to join in
     *   unconstrained levels so as to display extra columns
     * @param drillThrough Whether this is a request for a drill-through set
     */
    public CellRequest(
        RolapStar.Measure measure,
        boolean extendedContext,
        boolean drillThrough)
    {
        this.measure = measure;
        this.extendedContext = extendedContext;
        this.drillThrough = drillThrough;
        this.constrainedColumnsBitKey =
            BitKey.Factory.makeBitKey(measure.getStar().getColumnCount());
    }

    /**
     * Adds a constraint to this request.
     *
     * @param column Column to constraint
     * @param predicate Constraint to apply, or null to add column to the
     *   output without applying constraint
     */
    public final void addConstrainedColumn(
        RolapStar.Column column,
        StarColumnPredicate predicate)
    {
        // bitKeyPublished/segmentIdentity too: the duplicate-column branch
        // below never touches the frozen BitKey, so without this guard it
        // silently mutated predicates/unsatisfiable AFTER the key or the
        // identity was published
        if (columnsCache != null || segmentIdentity != null || bitKeyPublished) {
            throw new IllegalStateException(
                "addConstrainedColumn after the request was read");
        }

        // Sanity check; we should never be adding column constraints
        // from more than one star
        if (star == null) {
            star = column.getStar();
        } else {
            assert (star == column.getStar());
        }

        final int bitPosition = column.getBitPosition();
        if (this.constrainedColumnsBitKey.get(bitPosition)) {
            // This column is already constrained. Unless the value is the
            // same, or this value or the previous value is null (meaning
            // unconstrained) the request will never return any results.
            int index = indexOf(bitPosition);
            final StarColumnPredicate prevValue = predicates[index];
            if (prevValue == null) {
                // Previous column was unconstrained. Constrain on new
                // value.
            } else if (predicate == null) {
                // Previous column was constrained. Nothing to do.
                return;
            } else if (predicate.equalConstraint(prevValue)) {
                // Same constraint again. Nothing to do.
                return;
            } else {
                // Different constraint. Request is impossible to satisfy.
                predicate = null;
                unsatisfiable = true;
            }
            // Note: it is possible and valid for predicate to be null here
            predicates[index] = predicate;
            return;
        }
        this.constrainedColumnsBitKey.set(bitPosition);
        if (numColumns == columns.length) {
            columns = Arrays.copyOf(columns, numColumns * 2);
            predicates = Arrays.copyOf(predicates, numColumns * 2);
        }
        columns[numColumns] = column;
        predicates[numColumns] = predicate;
        numColumns++;
    }

    /** Slot of the already-constrained column; only reached on a duplicate add. */
    private int indexOf(int bitPosition) {
        for (int i = 0; i < numColumns; i++) {
            if (columns[i].getBitPosition() == bitPosition) {
                return i;
            }
        }
        throw new IllegalStateException("constrained column not found: bit " + bitPosition);
    }

    /**
     * Add compound member (formed via aggregate function) constraint to the
     * Cell.
     *
     * @param compoundBitKey Compound bit key
     * @param compoundPredicate Compound predicate
     */
    public void addAggregateList(
        BitKey compoundBitKey,
        StarPredicate compoundPredicate)
    {
        addAggregateList(compoundBitKey, compoundPredicate, null);
    }

    /**
     * As {@link #addAggregateList(BitKey, StarPredicate)}, taking the
     * predicate's wire form along when the caller has it already (the
     * slicer's is built once and reused across cell requests).
     */
    public void addAggregateList(
        BitKey compoundBitKey,
        StarPredicate compoundPredicate,
        SegmentPredicate wirePredicate)
    {
        if (compoundSegmentPredicates != null || segmentIdentity != null) {
            // mirror addConstrainedColumn's guard: the compound map feeds
            // three lazily-cached derivations - a post-read add silently
            // diverged the cached identity from the map
            throw new IllegalStateException(
                "addAggregateList after the request was read");
        }
        if (compoundPredicateMap == null) {
            compoundPredicateMap = new TreeMap<>();
        }
        // map keys are published state: only frozen keys go in
        final BitKey frozenKey = compoundBitKey.freeze();
        compoundPredicateMap.put(frozenKey, compoundPredicate);
        if (wirePredicate != null) {
            if (compoundWireMap == null) {
                compoundWireMap = new TreeMap<>();
            }
            compoundWireMap.put(frozenKey, wirePredicate);
        }
    }


    /**
     * Returns the measure of this cell request.
     *
     * @return Measure
     */
    public RolapStar.Measure getMeasure() {
        return measure;
    }

    public RolapStar.Column[] getConstrainedColumns() {
        if (this.columnsCache == null) {
            // This is called more than once so caching the value makes sense.
            freeze();
        }
        return this.columnsCache;
    }

    /**
     * Returns the BitKey for the list of columns.
     *
     * @return BitKey for the list of columns
     */
    public BitKey getConstrainedColumnsBitKey() {
        // freeze on first read: from here the key is published (working
        // store, batch identity, segment identity) and must never mutate
        // again - a later addConstrainedColumn now throws instead of
        // silently corrupting map buckets. Frozen keys return themselves,
        // so every further read is free.
        BitKey frozen = constrainedColumnsBitKey.freeze();
        constrainedColumnsBitKey = frozen;
        bitKeyPublished = true;
        return frozen;
    }

    /** The wire form of the compound predicates, derived once from the map. */
    public List<SegmentPredicate> getCompoundPredicates() {
        if (compoundSegmentPredicates == null) {
            if (compoundPredicateMap == null) {
                compoundSegmentPredicates = Collections.emptyList();
            } else {
                List<SegmentPredicate> wire =
                    new ArrayList<>(compoundPredicateMap.size());
                for (Map.Entry<BitKey, StarPredicate> entry
                    : compoundPredicateMap.entrySet())
                {
                    SegmentPredicate stored = compoundWireMap == null
                        ? null : compoundWireMap.get(entry.getKey());
                    wire.add(stored != null
                        ? stored : SegmentPredicates.toWire(entry.getValue()));
                }
                compoundSegmentPredicates = List.copyOf(wire);
            }
        }
        return compoundSegmentPredicates;
    }

    /** The runtime compound predicates in key order; constant, built once. */
    public List<StarPredicate> getCompoundPredicateList() {
        if (compoundPredicateList == null) {
            compoundPredicateList = compoundPredicateMap == null
                ? Collections.emptyList()
                : List.copyOf(compoundPredicateMap.values());
        }
        return compoundPredicateList;
    }

    /** The segment identity of this request; constant, computed once. */
    public SegmentIdentity segmentIdentity() {
        if (segmentIdentity == null) {
            final RolapStar requestStar = measure.getStar();
            segmentIdentity = new SegmentIdentity(
                requestStar.getCatalog().getName(),
                requestStar.getCatalog().getChecksum(),
                measure.getCubeName(),
                requestStar.getFactTable().getAlias(),
                measure.getName(),
                getCompoundPredicates(),
                // the GETTER, not the raw field: it freezes the key first,
                // so a later addConstrainedColumn throws instead of silently
                // diverging from the cached identity
                getConstrainedColumnsBitKey());
        }
        return segmentIdentity;
    }

    /**
     * Co-sorts the column and predicate arrays by bit position (insertion
     * sort over the few constrained columns) and freezes the request:
     * {@link #columnsCache} and {@link #columnBitPositions} come out in
     * ascending bit-position order, index-parallel to the predicates.
     */
    private void freeze() {
        if (isDirty) {
            for (int i = 1; i < numColumns; i++) {
                RolapStar.Column column = columns[i];
                StarColumnPredicate predicate = predicates[i];
                int bit = column.getBitPosition();
                int j = i - 1;
                while (j >= 0 && columns[j].getBitPosition() > bit) {
                    columns[j + 1] = columns[j];
                    predicates[j + 1] = predicates[j];
                    j--;
                }
                columns[j + 1] = column;
                predicates[j + 1] = predicate;
            }
            columnsCache = numColumns == columns.length
                ? columns : Arrays.copyOf(columns, numColumns);
            columnBitPositions = new int[numColumns];
            for (int i = 0; i < numColumns; i++) {
                columnBitPositions[i] = columnsCache[i].getBitPosition();
            }
            isDirty = false;
        }
    }

    /**
     * Return the predicate value associated with the given index.  Note that
     * index is different than bit position; if there are three constraints then
     * the indices are 0, 1, and 2, while the bitPositions could span a larger
     * range.
     *
     *  It is valid for the predicate at a given index to be null (there
     * should always be a column at that index, but it may not have an
     * associated predicate).
     *
     * @param index Index of the constraint we're looking up
     * @return predicate value associated with the given index
     */
    public StarColumnPredicate getValueAt(int index) {
        freeze();
        return predicates[index];
    }

    /**
     * Return the number of column constraints associated with this CellRequest.
     *
     * @return number of columns in the CellRequest
     */
    public int getNumValues() {
        freeze();
        return numColumns;
    }

    /**
     * Returns an array of the values for each column.
     *
     * The caller must check whether this request is satisfiable before
     * calling this method. May throw {@link NullPointerException} if request
     * is not satisfiable.
     *
     *  !isUnsatisfiable()
     * @return Array of values for each column
     */
    public Object[] getSingleValues() {
        assert !unsatisfiable;
        if (singleValues == null) {
            freeze();
            singleValues = new Object[numColumns];
            for (int i = 0; i < numColumns; i++) {
                ValueColumnPredicate predicate =
                    (ValueColumnPredicate) predicates[i];
                singleValues[i] = predicate.getValue();
            }
        }
        return singleValues;
    }

    /**
     * The request's coordinates as a map of column expressions to values:
     * an immutable view over the constrained columns. Lookups scan the few
     * columns with an identity fast path on the cached expressions.
     */
    public Map<String, Comparable> getMappedCellValues() {
        if (mappedCellValues == null) {
            mappedCellValues =
                new CellValuesView(getConstrainedColumns(), getSingleValues());
        }
        return mappedCellValues;
    }

    private static final class CellValuesView
            extends AbstractMap<String, Comparable> {
        private final RolapStar.Column[] columns;
        private final Object[] values;

        CellValuesView(RolapStar.Column[] columns, Object[] values) {
            this.columns = columns;
            this.values = values;
        }

        @Override
        public int size() {
            return columns.length;
        }

        @Override
        public boolean containsKey(Object key) {
            return indexOf(key) >= 0;
        }

        @Override
        public Comparable get(Object key) {
            int i = indexOf(key);
            return i >= 0 ? (Comparable) values[i] : null;
        }

        private int indexOf(Object key) {
            for (int i = 0; i < columns.length; i++) {
                String expression = columns[i].genericSql();
                if (expression == key || expression.equals(key)) {
                    return i;
                }
            }
            return -1;
        }

        @Override
        public Set<Entry<String, Comparable>> entrySet() {
            return new AbstractSet<>() {
                @Override
                public int size() {
                    return columns.length;
                }

                @Override
                public Iterator<Entry<String, Comparable>> iterator() {
                    return new Iterator<>() {
                        private int i;

                        @Override
                        public boolean hasNext() {
                            return i < columns.length;
                        }

                        @Override
                        public Entry<String, Comparable> next() {
                            if (i >= columns.length) {
                                throw new NoSuchElementException();
                            }
                            Entry<String, Comparable> entry = new SimpleImmutableEntry<>(
                                columns[i].genericSql(), (Comparable) values[i]);
                            i++;
                            return entry;
                        }
                    };
                }
            };
        }
    }

    /**
     * Returns whether this cell request is impossible to satisfy.
     * This occurs when the same column has two or more inconsistent
     * constraints.
     *
     * @return whether this cell request is impossible to satisfy
     */
    public boolean isUnsatisfiable() {
        return unsatisfiable;
    }
}
