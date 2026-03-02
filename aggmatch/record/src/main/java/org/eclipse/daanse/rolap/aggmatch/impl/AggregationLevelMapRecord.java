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

import org.eclipse.daanse.rolap.api.aggmatch.AggregationLevelMap;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRegex;

public record AggregationLevelMapRecord(String id, List<AggregationMatchRegex> regexes) implements AggregationLevelMap {

    public AggregationLevelMapRecord {
        regexes = regexes != null ? List.copyOf(regexes) : List.of();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public List<AggregationMatchRegex> getRegexes() {
        return regexes;
    }
}
