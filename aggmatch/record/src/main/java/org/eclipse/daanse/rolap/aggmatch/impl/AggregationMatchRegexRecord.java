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

import java.util.Objects;
import java.util.Optional;

import org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRegex;

public record AggregationMatchRegexRecord(String id, AggregationCharacterCase charCase, String template, String space,
        String dot) implements AggregationMatchRegex {

    public AggregationMatchRegexRecord {
        Objects.requireNonNull(template, "template");
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Optional<AggregationCharacterCase> getCharCase() {
        return Optional.ofNullable(charCase);
    }

    @Override
    public String getTemplate() {
        return template;
    }

    @Override
    public Optional<String> getSpace() {
        return Optional.ofNullable(space);
    }

    @Override
    public Optional<String> getDot() {
        return Optional.ofNullable(dot);
    }
}
