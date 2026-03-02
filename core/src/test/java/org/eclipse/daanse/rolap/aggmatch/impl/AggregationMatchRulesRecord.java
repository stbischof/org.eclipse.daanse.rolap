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

import java.util.List;

import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRule;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRules;

public record AggregationMatchRulesRecord(
    List<AggregationMatchRule> aggregationRules
) implements AggregationMatchRules {

    public AggregationMatchRulesRecord {
        aggregationRules = aggregationRules != null ? List.copyOf(aggregationRules) : List.of();
    }

    @Override public List<AggregationMatchRule> getAggregationRules() { return aggregationRules; }
}
