/*
 * Copyright (c) 2025 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.daanse.rolap.aggmatch.emf;

import java.util.List;

import org.eclipse.daanse.rolap.aggmatch.impl.AggregationFactCountMatchRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationForeignKeyMatchRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationIgnoreMapRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationLevelMapRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationMatchRegexRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationMatchRuleRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationMatchRulesRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationMeasureMapRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationTableMatchRecord;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationFactCountMatch;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationForeignKeyMatch;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationIgnoreMap;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationLevelMap;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRegex;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRule;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRules;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMeasureMap;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationTableMatch;

/**
 * Converts EMF-generated aggmatch objects to plain Java records. Only this
 * module depends on EMF; consumers work with plain interfaces.
 */
public class EmfAggMatchConverter {

    public static AggregationMatchRules convert(
            org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationMatchRules emfRules) {
        List<AggregationMatchRule> rules = emfRules.getAggregationRules().stream()
                .map(EmfAggMatchConverter::convertRule).toList();
        return new AggregationMatchRulesRecord(rules);
    }

    private static AggregationMatchRule convertRule(
            org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationMatchRule emf) {
        return new AggregationMatchRuleRecord(emf.getTag(), emf.getEnabled(),
                emf.getTableMatch() != null ? convertTableMatch(emf.getTableMatch()) : null,
                emf.getFactCountMatch() != null ? convertFactCountMatch(emf.getFactCountMatch()) : null,
                emf.getForeignKeyMatch() != null ? convertForeignKeyMatch(emf.getForeignKeyMatch()) : null,
                emf.getLevelMap() != null ? convertLevelMap(emf.getLevelMap()) : null,
                emf.getMeasureMap() != null ? convertMeasureMap(emf.getMeasureMap()) : null,
                emf.getIgnoreMap() != null ? convertIgnoreMap(emf.getIgnoreMap()) : null);
    }

    private static AggregationCharacterCase convertCharCase(
            org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationCharacterCase emf) {
        if (emf == null) {
            return null;
        }
        try {
            return AggregationCharacterCase.valueOf(emf.name());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown EMF AggregationCharacterCase literal: " + emf.name(), e);
        }
    }

    private static AggregationMatchRegex convertRegex(
            org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationMatchRegex emf) {
        return new AggregationMatchRegexRecord(emf.getId(), convertCharCase(emf.getCharCase()), emf.getTemplate(),
                emf.getSpace(), emf.getDot());
    }

    private static List<AggregationMatchRegex> convertRegexes(
            List<? extends org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationMatchRegex> emfList) {
        if (emfList == null) {
            return List.of();
        }
        return emfList.stream().map(EmfAggMatchConverter::convertRegex).toList();
    }

    private static AggregationTableMatch convertTableMatch(
            org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationTableMatch emf) {
        return new AggregationTableMatchRecord(emf.getId(), convertCharCase(emf.getCharCase()), emf.getPretemplate(),
                emf.getPosttemplate(), emf.getBasename());
    }

    private static AggregationFactCountMatch convertFactCountMatch(
            org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationFactCountMatch emf) {
        return new AggregationFactCountMatchRecord(emf.getId(), convertCharCase(emf.getCharCase()),
                emf.getPretemplate(), emf.getPosttemplate(), emf.getBasename(), emf.getFactCountName());
    }

    private static AggregationForeignKeyMatch convertForeignKeyMatch(
            org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationForeignKeyMatch emf) {
        return new AggregationForeignKeyMatchRecord(emf.getId(), convertCharCase(emf.getCharCase()),
                emf.getPretemplate(), emf.getPosttemplate(), emf.getBasename());
    }

    private static AggregationLevelMap convertLevelMap(
            org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationLevelMap emf) {
        return new AggregationLevelMapRecord(emf.getId(), convertRegexes(emf.getRegexes()));
    }

    private static AggregationMeasureMap convertMeasureMap(
            org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationMeasureMap emf) {
        return new AggregationMeasureMapRecord(emf.getId(), convertRegexes(emf.getRegexes()));
    }

    private static AggregationIgnoreMap convertIgnoreMap(
            org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationIgnoreMap emf) {
        return new AggregationIgnoreMapRecord(emf.getId(), convertRegexes(emf.getRegexes()));
    }

    private EmfAggMatchConverter() {
    }
}
