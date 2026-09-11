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
package org.eclipse.daanse.rolap.common.star;

import java.util.List;
import java.util.Map;

import org.eclipse.daanse.olap.key.BitKey;
import org.eclipse.daanse.olap.spi.SegmentColumn;
import org.eclipse.daanse.olap.spi.SegmentHeader;
import org.eclipse.daanse.olap.spi.SegmentRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Renders bit keys and segment headers human-readable for audits: which
 * columns a dimensionality stands for, and in which form a segment
 * constrains them. A raw {@link BitKey} is just positions — the meaning
 * lives in the {@link RolapStar} (bit position → column); a
 * {@link SegmentHeader} is self-describing (its columns carry their
 * expressions and value sets).
 *
 * Off by default: every emitting call site guards with {@link #enabled()},
 * so the cost of the disabled state is one logger-level check. Enable by
 * setting the logger {@value #LOGGER_NAME} to DEBUG — cache decisions
 * (segment publications, batch loads, rollup candidate searches) then
 * explain themselves to that logger. The render methods are public and
 * side-effect-free, so audits and tests can also call them directly.
 */
public final class BitKeyExplain {

    /** Central switch: DEBUG here turns the cache explanations on. */
    public static final String LOGGER_NAME = "daanse.cache.explain";

    public static final Logger EXPLAIN = LoggerFactory.getLogger(LOGGER_NAME);

    private BitKeyExplain() {
    }

    /** Whether call sites should render and emit explanations. */
    public static boolean enabled() {
        return EXPLAIN.isDebugEnabled();
    }

    /**
     * The dimensionality as column expressions with their bit positions:
     * {@code {store.store_country#7, time.the_year#21}}. Positions the
     * star does not know (never the case for keys built against it) are
     * rendered as {@code ?#pos}.
     */
    public static String explain(RolapStar star, BitKey bitKey) {
        StringBuilder sb = new StringBuilder("{");
        boolean[] first = {true};
        bitKey.forEachSetBit(pos -> {
            if (!first[0]) {
                sb.append(", ");
            }
            first[0] = false;
            // getColumn throws beyond the column list (ArrayList.get), and
            // wire-decoded keys can carry bits this star does not know -
            // a diagnostic line must degrade, never abort the caller
            RolapStar.Column column =
                pos < star.getColumnCount() ? star.getColumn(pos) : null;
            String expression = column == null ? null : column.genericSql();
            sb.append(expression == null ? "?" : expression);
            sb.append('#').append(pos);
        });
        return sb.append('}').toString();
    }

    /**
     * The segment as measure@fact plus every constrained column with the
     * FORM of its constraint: {@code =value} (one value),
     * {@code in(n values)} (an enumerated set), or {@code =*} (wildcard —
     * the whole axis); excluded regions append as {@code -box{...}}.
     */
    public static String explain(SegmentHeader header) {
        StringBuilder sb = new StringBuilder();
        sb.append(header.measureName).append('@')
            .append(header.rolapStarFactTableName).append(' ');
        appendColumns(sb, header.getConstrainedColumns());
        for (SegmentRegion region : header.getExcludedRegions()) {
            sb.append(" -box");
            appendColumns(sb, region.columns());
        }
        return sb.toString();
    }

    /** One rollup request: the target coordinates, self-describing. */
    public static String explain(Map<String, Comparable> coordinates) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Comparable> entry : coordinates.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(entry.getKey()).append('=').append(entry.getValue());
        }
        return sb.append('}').toString();
    }

    private static void appendColumns(StringBuilder sb, List<SegmentColumn> columns) {
        sb.append('{');
        boolean first = true;
        for (SegmentColumn column : columns) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(column.columnExpression);
            if (column.values == null) {
                sb.append("=*");
            } else if (column.values.size() == 1) {
                sb.append('=').append(column.values.first());
            } else {
                sb.append("=in(").append(column.values.size()).append(" values)");
            }
        }
        sb.append('}');
    }
}
