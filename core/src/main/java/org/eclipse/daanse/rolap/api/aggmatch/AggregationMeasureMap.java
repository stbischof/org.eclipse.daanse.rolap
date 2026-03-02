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
 * Regex mapper for recognizing measure columns in aggregate tables.
 * Templates use variables like {@code ${measure_name}}, {@code ${measure_column_name}},
 * and {@code ${aggregate_name}}.
 */
public interface AggregationMeasureMap extends AggregationMatchRegexMapper {
}
