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
 * Controls how character case is handled during aggregate column name matching.
 */
public enum AggregationCharacterCase {
    /** Case-insensitive matching ({@code Pattern.CASE_INSENSITIVE}). */
    IGNORE,
    /** Case-sensitive matching — names are used as-is. */
    EXACT,
    /** Names are converted to uppercase before matching. */
    UPPER,
    /** Names are converted to lowercase before matching. */
    LOWER
}
