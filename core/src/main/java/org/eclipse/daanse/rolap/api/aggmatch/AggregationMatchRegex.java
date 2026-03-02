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
 * A single regex pattern configuration with template variable substitution.
 * Template variables like {@code ${measure_name}} are replaced with actual
 * metadata values at matching time.
 */
public interface AggregationMatchRegex {

    String getId();

    Optional<AggregationCharacterCase> getCharCase();

    String getTemplate();

    /** Replacement string for spaces in names (defaults to "_"). */
    Optional<String> getSpace();

    /** Replacement string for dots in names (defaults to "_"). */
    Optional<String> getDot();
}
