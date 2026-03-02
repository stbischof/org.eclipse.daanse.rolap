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

import java.util.Optional;

import org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationForeignKeyMatch;

public record AggregationForeignKeyMatchRecord(String id, AggregationCharacterCase charCase, String prefixRegex,
        String suffixRegex, String nameExtractRegex) implements AggregationForeignKeyMatch {

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Optional<AggregationCharacterCase> getCharCase() {
        return Optional.ofNullable(charCase);
    }

    @Override
    public Optional<String> getPrefixRegex() {
        return Optional.ofNullable(prefixRegex);
    }

    @Override
    public Optional<String> getSuffixRegex() {
        return Optional.ofNullable(suffixRegex);
    }

    @Override
    public Optional<String> getNameExtractRegex() {
        return Optional.ofNullable(nameExtractRegex);
    }
}
