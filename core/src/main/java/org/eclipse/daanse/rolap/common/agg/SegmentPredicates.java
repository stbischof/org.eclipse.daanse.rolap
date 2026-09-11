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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.eclipse.daanse.olap.spi.SegmentPredicate;
import org.eclipse.daanse.olap.common.Util;
import org.eclipse.daanse.rolap.common.star.RolapStar;
import org.eclipse.daanse.rolap.common.star.StarColumnPredicate;
import org.eclipse.daanse.rolap.common.star.StarPredicate;

/**
 * Translates between runtime {@link StarPredicate}s and the wire form
 * {@link SegmentPredicate}. The wire form is canonical (sorted list values,
 * sorted and/or children) and dialect-free, so equal logical predicates
 * produce equal segment ids on every node. Predicate shapes without a
 * structural mapping travel as {@link SegmentPredicate.Opaque} built from
 * the canonical ANSI rendering; those reconstruct only through a registered
 * converter.
 */
public final class SegmentPredicates {

    private SegmentPredicates() {
    }

    private static final Comparator<Comparable> VALUE_ORDER =
            Comparator.nullsFirst(Comparator.comparing(String::valueOf));

    public static List<SegmentPredicate> toWire(List<StarPredicate> predicates) {
        List<SegmentPredicate> result = new ArrayList<>(predicates.size());
        for (StarPredicate predicate : predicates) {
            result.add(toWire(predicate));
        }
        return result;
    }

    public static SegmentPredicate toWire(StarPredicate predicate) {
        if (predicate instanceof ValueColumnPredicate value) {
            return new SegmentPredicate.Value(expression(value), wireValue(value.getValue()));
        }
        if (predicate instanceof ListColumnPredicate list) {
            List<Comparable> values = new ArrayList<>(list.getPredicates().size());
            for (StarColumnPredicate child : list.getPredicates()) {
                if (!(child instanceof ValueColumnPredicate value)) {
                    return opaque(predicate);
                }
                values.add(wireValue(value.getValue()));
            }
            values.sort(VALUE_ORDER);
            return new SegmentPredicate.Values(expression(list), values);
        }
        if (predicate instanceof RangeColumnPredicate range) {
            return new SegmentPredicate.Range(
                    expression(range),
                    range.getLowerBound() == null ? null : wireValue(range.getLowerBound().getValue()),
                    range.getLowerInclusive(),
                    range.getUpperBound() == null ? null : wireValue(range.getUpperBound().getValue()),
                    range.getUpperInclusive());
        }
        if (predicate instanceof MemberTuplePredicate tuple) {
            return tupleToWire(tuple);
        }
        if (predicate instanceof MinusStarPredicate minus) {
            return new SegmentPredicate.And(List.of(
                    toWire(minus.getPlus()),
                    new SegmentPredicate.Not(toWire(minus.getMinus()))));
        }
        if (predicate instanceof AndPredicate and) {
            return new SegmentPredicate.And(children(and.getChildren()));
        }
        if (predicate instanceof OrPredicate or) {
            return new SegmentPredicate.Or(children(or.getChildren()));
        }
        if (predicate instanceof LiteralStarPredicate literal) {
            return new SegmentPredicate.Literal(literal.getValue());
        }
        return opaque(predicate);
    }

    /**
     * Rebuilds runtime predicates from the wire form against a star; empty
     * when any predicate has no structural mapping (opaque or negation).
     */
    public static Optional<List<StarPredicate>> toStarPredicates(
            List<SegmentPredicate> predicates, RolapStar star) {
        List<StarPredicate> result = new ArrayList<>(predicates.size());
        for (SegmentPredicate predicate : predicates) {
            StarPredicate star1 = toStarPredicate(predicate, star);
            if (star1 == null) {
                return Optional.empty();
            }
            result.add(star1);
        }
        return Optional.of(result);
    }

    private static StarPredicate toStarPredicate(SegmentPredicate predicate, RolapStar star) {
        if (predicate instanceof SegmentPredicate.Value value) {
            RolapStar.Column column = findColumn(star, value.columnExpression());
            return column == null ? null
                    : new ValueColumnPredicate(column, starValue(value.value()));
        }
        if (predicate instanceof SegmentPredicate.Values values) {
            RolapStar.Column column = findColumn(star, values.columnExpression());
            if (column == null) {
                return null;
            }
            List<StarColumnPredicate> children = new ArrayList<>(values.values().size());
            for (Comparable value : values.values()) {
                children.add(new ValueColumnPredicate(column, starValue(value)));
            }
            return new ListColumnPredicate(column, children);
        }
        if (predicate instanceof SegmentPredicate.Range range) {
            RolapStar.Column column = findColumn(star, range.columnExpression());
            return column == null ? null
                    : new RangeColumnPredicate(column,
                            range.lowerInclusive(),
                            range.lower() == null ? null
                                    : new ValueColumnPredicate(column, starValue(range.lower())),
                            range.upperInclusive(),
                            range.upper() == null ? null
                                    : new ValueColumnPredicate(column, starValue(range.upper())));
        }
        if (predicate instanceof SegmentPredicate.And and) {
            List<StarPredicate> children = childStarPredicates(and.children(), star);
            return children == null ? null : new AndPredicate(children);
        }
        if (predicate instanceof SegmentPredicate.Or or) {
            List<StarPredicate> children = childStarPredicates(or.children(), star);
            return children == null ? null : new OrPredicate(children);
        }
        if (predicate instanceof SegmentPredicate.Literal literal) {
            return new LiteralStarPredicate(null, literal.value());
        }
        // Opaque and Not carry no structural mapping
        return null;
    }

    private static List<StarPredicate> childStarPredicates(
            List<SegmentPredicate> children, RolapStar star) {
        List<StarPredicate> result = new ArrayList<>(children.size());
        for (SegmentPredicate child : children) {
            StarPredicate starChild = toStarPredicate(child, star);
            if (starChild == null) {
                return null;
            }
            result.add(starChild);
        }
        return result;
    }

    private static RolapStar.Column findColumn(RolapStar star, String columnExpression) {
        for (int bit = 0; bit < star.getColumnCount(); bit++) {
            RolapStar.Column column = star.getColumn(bit);
            if (column != null && columnExpression.equals(column.genericSql())) {
                return column;
            }
        }
        return null;
    }

    /** Wire children in canonical order; the sort key computes once per child. */
    private static List<SegmentPredicate> children(List<StarPredicate> children) {
        record Keyed(String key, SegmentPredicate predicate) {
        }
        List<Keyed> keyed = new ArrayList<>(children.size());
        for (StarPredicate child : children) {
            SegmentPredicate wire = toWire(child);
            keyed.add(new Keyed(wire.canonical(), wire));
        }
        keyed.sort(Comparator.comparing(Keyed::key));
        List<SegmentPredicate> result = new ArrayList<>(keyed.size());
        for (Keyed k : keyed) {
            result.add(k.predicate());
        }
        return result;
    }

    private static String expression(StarColumnPredicate predicate) {
        return predicate.getConstrainedColumn().genericSql();
    }

    private static Comparable wireValue(Object value) {
        return value == Util.sqlNullValue ? null : (Comparable) value;
    }

    private static Object starValue(Comparable value) {
        return value == null ? Util.sqlNullValue : value;
    }

    /**
     * A member-tuple range expands into the same lexicographic and/or/range
     * tree the SQL translation produces; the child order follows the tuple
     * columns and is deterministic, so it is not re-sorted.
     */
    private static SegmentPredicate tupleToWire(MemberTuplePredicate tuple) {
        List<RolapStar.Column> columns = tuple.getConstrainedColumnList();
        List<SegmentPredicate> boundPreds = new ArrayList<>();
        for (MemberTuplePredicate.BoundSpec bound : tuple.getBoundSpecs()) {
            boundPreds.add(boundToWire(columns, bound));
        }
        return boundPreds.size() == 1 ? boundPreds.get(0) : new SegmentPredicate.And(boundPreds);
    }

    private static SegmentPredicate boundToWire(
            List<RolapStar.Column> columns, MemberTuplePredicate.BoundSpec bound) {
        int n = columns.size();
        List<Object> values = bound.values();
        if (bound.relation() == MemberTuplePredicate.BoundRelation.EQ) {
            List<SegmentPredicate> eqs = new ArrayList<>(n);
            for (int k = 0; k < n; k++) {
                eqs.add(columnEquals(columns.get(k), values.get(k)));
            }
            return eqs.size() == 1 ? eqs.get(0) : new SegmentPredicate.And(eqs);
        }
        boolean greater = bound.relation() == MemberTuplePredicate.BoundRelation.GT
                || bound.relation() == MemberTuplePredicate.BoundRelation.GE;
        boolean finalInclusive = bound.relation() == MemberTuplePredicate.BoundRelation.GE
                || bound.relation() == MemberTuplePredicate.BoundRelation.LE;
        List<SegmentPredicate> terms = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            List<SegmentPredicate> conjunction = new ArrayList<>(k + 1);
            for (int j = 0; j < k; j++) {
                conjunction.add(columnEquals(columns.get(j), values.get(j)));
            }
            boolean inclusive = k == n - 1 && finalInclusive;
            conjunction.add(columnBound(columns.get(k), values.get(k), greater, inclusive));
            terms.add(conjunction.size() == 1 ? conjunction.get(0)
                    : new SegmentPredicate.And(conjunction));
        }
        return terms.size() == 1 ? terms.get(0) : new SegmentPredicate.Or(terms);
    }

    private static SegmentPredicate columnEquals(RolapStar.Column column, Object value) {
        return new SegmentPredicate.Value(column.genericSql(), wireValue(value));
    }

    private static SegmentPredicate columnBound(
            RolapStar.Column column, Object value, boolean greater, boolean inclusive) {
        String expression = column.genericSql();
        return greater
                ? new SegmentPredicate.Range(expression, wireValue(value), inclusive, null, false)
                : new SegmentPredicate.Range(expression, null, false, wireValue(value), inclusive);
    }

    private static SegmentPredicate.Opaque opaque(StarPredicate predicate) {
        return new SegmentPredicate.Opaque(String.valueOf(predicate));
    }
}
