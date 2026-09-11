/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2001-2005 Julian Hyde
 * Copyright (C) 2005-2017 Hitachi Vantara and others
 * All Rights Reserved.
 *
 * jhyde, 28 August, 2001
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.eclipse.daanse.olap.api.catalog.CatalogReader;
import org.eclipse.daanse.olap.api.element.Level;
import org.eclipse.daanse.olap.api.element.Member;
import org.eclipse.daanse.olap.common.Util;
import org.eclipse.daanse.olap.key.BitKey;
import org.eclipse.daanse.rolap.common.result.GroupingSetsCollector;
import org.eclipse.daanse.rolap.common.star.RolapStar;
import org.eclipse.daanse.rolap.common.star.StarColumnPredicate;
import org.eclipse.daanse.rolap.common.star.StarPredicate;

/**
 * Per-batch helper around one (star, constrained columns, compound
 * predicates) shape: it optimizes the predicates and constructs the
 * segments a load produces. Instances are created per batch and thrown
 * away — no aggregation state is cached here (segments live in the
 * cache manager's index and stores).
 */
public class Aggregation {

    private final List<StarPredicate> compoundPredicateList;
    private final RolapStar star;
    private final BitKey constrainedColumnsBitKey;

    /**
     * Setting for optimizing SQL predicates.
     */
    private final int maxConstraints;

    /**
     * Creates an Aggregation.
     *
     * @param star Star this aggregation belongs to
     * @param constrainedColumnsBitKey Constrained columns (the dimensionality)
     * @param compoundPredicateList Compound predicates
     * @param maxConstraints Setting for optimizing SQL predicates
     */
    public Aggregation(
        RolapStar star, BitKey constrainedColumnsBitKey,
        List<StarPredicate> compoundPredicateList, final int maxConstraints)
    {
        this.compoundPredicateList = compoundPredicateList;
        this.star = star;
        this.constrainedColumnsBitKey = constrainedColumnsBitKey;
        this.maxConstraints = maxConstraints;
    }

    /**
     * Loads a set of segments into this aggregation, one per measure,
     * each constrained by the same set of column values.
     *
     * A Column and its constraints are accessed at the same level in their
     * respective arrays.
     *
     * For example,
     *
     * measures = {unit_sales, store_sales},
     * state = {CA, OR},
     * gender = unconstrained
     *
     * @param segmentFutures List of futures wherein each statement will place
     *                       a list of the segments it has loaded, when it
     *                       completes
     */
    public void load(
        SegmentCacheManager cacheMgr,
        int cellRequestCount,
        RolapStar.Column[] columns,
        List<RolapStar.Measure> measures,
        StarColumnPredicate[] predicates,
        GroupingSetsCollector groupingSetsCollector,
        List<Future<Map<Segment, SegmentWithData>>> segmentFutures)
    {
        BitKey measureBitKey = getConstrainedColumnsBitKey().emptyCopy();
        int axisCount = columns.length;
        Util.assertTrue(predicates.length == axisCount);

        List<Segment> segments =
            createSegments(
                columns, measures, measureBitKey, predicates);

        // The constrained columns are simply the level and foreign columns
        BitKey levelBitKey = getConstrainedColumnsBitKey();
        GroupingSet groupingSet =
            new GroupingSet(
                segments, levelBitKey, measureBitKey, predicates, columns);
        if (groupingSetsCollector.useGroupingSets()) {
            groupingSetsCollector.add(groupingSet);
        } else {
            final SegmentLoader segmentLoader = new SegmentLoader(cacheMgr);
            segmentLoader.load(
                cellRequestCount,
                new ArrayList<>(
                    Collections.singletonList(groupingSet)),
                compoundPredicateList,
                segmentFutures);
        }
    }

    private List<Segment> createSegments(
        RolapStar.Column[] columns,
        List<RolapStar.Measure> measures,
        BitKey measureBitKey,
        StarColumnPredicate[] predicates)
    {
        List<Segment> segments = new ArrayList<>(measures.size());
        for (RolapStar.Measure measure : measures) {
            measureBitKey.set(measure.getBitPosition());
            Segment segment =
                new Segment(
                    star,
                    constrainedColumnsBitKey,
                    columns,
                    measure,
                    predicates,
                    Collections.<Segment.ExcludedRegion>emptyList(),
                    compoundPredicateList);
            segments.add(segment);
        }
        // It is important to sort the segments per measure bitkey.
        // The order in which the measures come in is not deterministic.
        // It actually depends on the order of the CellRequests.
        // See: org.eclipse.daanse.rolap.common.result.BatchLoader.Batch#add(CellRequest).
        // Failure to sort them will give out wrong results (uses the wrong
        // column) if we have more than one column in the grouping set.
        Collections.sort(segments,
                (o1, o2) -> Integer.compare(o1.measure.getBitPosition(), o2.measure.getBitPosition()));
        return segments;
    }

    /**
     * Drops predicates, where the list of values is close to the values which
     * would be returned anyway.
     */
    public StarColumnPredicate[] optimizePredicates(
        RolapStar.Column[] columns,
        StarColumnPredicate[] predicates,
        boolean optimizePredicates)
    {
        RolapStar star = getStar();
        Util.assertTrue(predicates.length == columns.length);
        StarColumnPredicate[] newPredicates = predicates.clone();
        double[] bloats = new double[columns.length];

        // We want to handle the special case "drilldown" which occurs pretty
        // often. Here, the parent is here as a constraint with a single member
        // and the list of children as well.
        List<Member> potentialParents = new ArrayList<>();
        for (final StarColumnPredicate predicate : predicates) {
            Member m;
            if (predicate instanceof MemberColumnPredicate memberColumnPredicate) {
                m = memberColumnPredicate.getMember();
                potentialParents.add(m);
            }
        }

        for (int i = 0; i < newPredicates.length; i++) {
            // A set of constraints with only one entry will not be optimized
            // away
            if (!(newPredicates[i] instanceof ListColumnPredicate newPredicate)) {
                bloats[i] = 0.0;
                continue;
            }

            final List<StarColumnPredicate> predicateList =
                newPredicate.getPredicates();
            final int valueCount = predicateList.size();
            if (valueCount < 2) {
                bloats[i] = 0.0;
                continue;
            }

            if (valueCount > maxConstraints) {
                // Some databases can handle only a limited number of elements
                // in 'WHERE IN (...)'. This set is greater than this database
                // can handle, so we drop this constraint. Hopefully there are
                // other constraints that will limit the result.
                bloats[i] = 1.0; // will be optimized away
                continue;
            }

            // more than one - check for children of same parent
            double constraintLength = valueCount;
            Member parent = null;
            Level level = null;
            for (int j = 0; j < valueCount; j++) {
                Object value = predicateList.get(j);
                if (value instanceof MemberColumnPredicate memberColumnPredicate) {
                    Member m = memberColumnPredicate.getMember();
                    if (j == 0) {
                        parent = m.getParentMember();
                        level = m.getLevel();
                    } else {
                        if (parent != null
                            && !parent.equals(m.getParentMember()))
                        {
                            parent = null; // no common parent
                        }
                        if (level != null
                            && !level.equals(m.getLevel()))
                        {
                            // should never occur, constraints are of same level
                            level = null;
                        }
                    }
                } else {
                    // Value constraint with no associated member.
                    // Compute bloat by #constraints / column cardinality.
                    parent = null;
                    level = null;
                    bloats[i] = constraintLength / columns[i].getCardinality();
                    break;
                }
            }
            boolean done = false;
            if (parent != null && (parent.isAll() || potentialParents.contains(parent))) {
                // common parent exists
                // common parent is there as constraint
                //  if the children are complete, this constraint set is
                //  unneccessary try to get the children directly from
                //  cache for the drilldown case, the children will be
                //  in the cache
                // - if not, forget this optimization.
                CatalogReader scr = star.getCatalog().getCatalogReaderWithDefaultRole();
                int childCount = scr.getChildrenCountFromCache(parent);
                if (childCount == -1) {
                   // nothing gotten from cache
                   if (!parent.isAll()) {
                       // parent is in constraints
                       // no information about children cardinality
                       //  constraints must not be optimized away
                       bloats[i] = 0.0;
                       done = true;
                   }
                } else {
                    bloats[i] = constraintLength / childCount;
                    done = true;
                }
            }

            if (!done && level != null) {
                // if the level members are cached, we do not need "count *"
                CatalogReader scr = star.getCatalog().getCatalogReaderWithDefaultRole();
                int memberCount = scr.getLevelCardinality(level, true, false);
                if (memberCount > 0) {
                    bloats[i] = constraintLength / memberCount;
                    done = true;
                }
            }

            if (!done) {
                bloats[i] = constraintLength / columns[i].getCardinality();
            }
        }

        // build a list of constraints sorted by 'bloat factor'
        ConstraintComparator comparator = new ConstraintComparator(bloats);
        Integer[] indexes = new Integer[columns.length];
        for (int i = 0; i < columns.length; i++) {
            indexes[i] = i;
        }

        // sort indexes by bloat descending
        Arrays.sort(indexes, comparator);

        // Eliminate constraints one by one, until the constrained cell count
        // became half of the unconstrained cell count. We can not have an
        // absolute value here, because its
        // very different if we fetch data for 2 years or 10 years (5 times
        // more means 5 times slower). So a relative comparison is ok here
        // but not an absolute one.

        double abloat = 1.0;
        final double aBloatLimit = .5;

        for (Integer j : indexes) {
            abloat = abloat * bloats[j];
            if (abloat <= aBloatLimit) {
                break;
            }
            // eliminate this constraint
            if (optimizePredicates
                || bloats[j] == 1)
            {
                newPredicates[j] = new LiteralStarPredicate(columns[j], true);
            }
        }

        // Now do simple structural optimizations, e.g. convert a list predicate
        // with one element to a value predicate.
        for (int i = 0; i < newPredicates.length; i++) {
            newPredicates[i] = StarPredicates.optimize(newPredicates[i]);
        }

        return newPredicates;
    }

    /**
     * This is called during SQL generation.
     */
    public RolapStar getStar() {
        return star;
    }

    /**
     * Returns the BitKey for ALL columns (Measures and Levels) involved in the
     * query.
     */
    public BitKey getConstrainedColumnsBitKey() {
        return constrainedColumnsBitKey;
    }

    // -- classes -------------------------------------------------------------

    private static class ConstraintComparator implements Comparator<Integer> {
        private final double[] bloats;

        ConstraintComparator(double[] bloats) {
            this.bloats = bloats;
        }

        // implement Comparator
        // order by bloat descending
        @Override
		public int compare(Integer o0, Integer o1) {
            double bloat0 = bloats[o0];
            double bloat1 = bloats[o1];
            if (bloat0 == bloat1) {
                return 0;
            } else {
                return (bloat0 < bloat1) ? 1 : -1;
            }
        }
    }


}
