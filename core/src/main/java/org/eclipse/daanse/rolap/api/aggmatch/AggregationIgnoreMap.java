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
 * Regex mapper for columns that should be ignored during aggregate table recognition.
 * Matched columns are marked as IGNORE and excluded from further validation.
 */
public interface AggregationIgnoreMap extends AggregationMatchRegexMapper {
}
