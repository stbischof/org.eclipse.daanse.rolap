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

import java.util.List;

/**
 * Top-level container for aggregate table matching rules.
 * Each rule defines how to recognize aggregate tables, their columns,
 * and how to map them to measures, levels, and foreign keys.
 */
public interface AggregationMatchRules {

    List<? extends AggregationMatchRule> getAggregationRules();
}
