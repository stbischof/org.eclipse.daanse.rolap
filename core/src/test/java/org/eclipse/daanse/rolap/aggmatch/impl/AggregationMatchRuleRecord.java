/*
 * Copyright (c) 2025 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.daanse.rolap.aggmatch.impl;

import java.util.Objects;
import java.util.Optional;

import org.eclipse.daanse.rolap.api.aggmatch.AggregationFactCountMatch;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationForeignKeyMatch;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationIgnoreMap;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationLevelMap;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRule;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMeasureMap;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationTableMatch;

public record AggregationMatchRuleRecord(
    String tag,
    Boolean enabled,
    AggregationTableMatch tableMatch,
    AggregationFactCountMatch factCountMatch,
    AggregationForeignKeyMatch foreignKeyMatch,
    AggregationLevelMap levelMap,
    AggregationMeasureMap measureMap,
    AggregationIgnoreMap ignoreMap
) implements AggregationMatchRule {

    public AggregationMatchRuleRecord {
        Objects.requireNonNull(tag, "tag");
        Objects.requireNonNull(tableMatch, "tableMatch");
        Objects.requireNonNull(factCountMatch, "factCountMatch");
        Objects.requireNonNull(foreignKeyMatch, "foreignKeyMatch");
        Objects.requireNonNull(levelMap, "levelMap");
        Objects.requireNonNull(measureMap, "measureMap");
    }

    @Override public String getTag() { return tag; }
    @Override public Optional<Boolean> getEnabled() { return Optional.ofNullable(enabled); }
    @Override public AggregationTableMatch getTableMatch() { return tableMatch; }
    @Override public AggregationFactCountMatch getFactCountMatch() { return factCountMatch; }
    @Override public AggregationForeignKeyMatch getForeignKeyMatch() { return foreignKeyMatch; }
    @Override public AggregationLevelMap getLevelMap() { return levelMap; }
    @Override public AggregationMeasureMap getMeasureMap() { return measureMap; }
    @Override public Optional<AggregationIgnoreMap> getIgnoreMap() { return Optional.ofNullable(ignoreMap); }
}
