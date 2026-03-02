/*
 * Copyright (c) 2025 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.daanse.rolap.api.aggmatch;

import java.util.Optional;

/**
 * A single aggregate table matching rule identified by a tag.
 * Combines matchers for table names, fact count columns, foreign keys,
 * levels, measures, and optionally ignored columns.
 */
public interface AggregationMatchRule {

    String getTag();

    /**
     * Whether this rule is enabled. If empty, the rule defaults to enabled.
     * A rule explicitly set to {@code false} is skipped during matching.
     */
    Optional<Boolean> getEnabled();

    AggregationTableMatch getTableMatch();

    AggregationFactCountMatch getFactCountMatch();

    AggregationForeignKeyMatch getForeignKeyMatch();

    AggregationLevelMap getLevelMap();

    AggregationMeasureMap getMeasureMap();

    Optional<AggregationIgnoreMap> getIgnoreMap();
}
