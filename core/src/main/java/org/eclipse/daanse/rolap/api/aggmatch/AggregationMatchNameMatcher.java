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
 * Matcher for table, foreign key, or fact count column names.
 *
 * The matching process:
 * 1. If nameExtractRegex is present, apply it to extract capture group(1)
 * 2. Concatenate: prefixRegex + extractedName + suffixRegex
 * 3. Match against candidate names (case handling per charCase)
 */
public interface AggregationMatchNameMatcher {

    String getId();

    Optional<AggregationCharacterCase> getCharCase();

    Optional<String> getPrefixRegex();

    Optional<String> getSuffixRegex();

    /**
     * Optional regex pattern to extract a base name from the input.
     * If present, capture group(1) replaces the full name in the pattern.
     */
    Optional<String> getNameExtractRegex();
}
