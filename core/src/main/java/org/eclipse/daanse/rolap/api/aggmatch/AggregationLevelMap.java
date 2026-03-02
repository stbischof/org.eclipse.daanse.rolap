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

/**
 * Regex mapper for recognizing level columns in aggregate tables.
 * Templates use variables like {@code ${hierarchy_name}}, {@code ${level_name}},
 * {@code ${level_column_name}}, and {@code ${usage_prefix}}.
 */
public interface AggregationLevelMap extends AggregationMatchRegexMapper {
}
