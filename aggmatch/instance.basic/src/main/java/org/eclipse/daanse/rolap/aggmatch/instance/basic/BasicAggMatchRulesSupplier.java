/*
 * Copyright (c) 2025 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.daanse.rolap.aggmatch.instance.basic;

import static org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase.EXACT;
import static org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase.LOWER;

import java.util.List;

import org.eclipse.daanse.rolap.aggmatch.impl.AggregationFactCountMatchRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationForeignKeyMatchRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationLevelMapRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationMatchRegexRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationMatchRuleRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationMatchRulesRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationMeasureMapRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationTableMatchRecord;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRules;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRulesSupplier;
import org.osgi.service.component.annotations.Component;

@Component(service = AggregationMatchRulesSupplier.class)
public class BasicAggMatchRulesSupplier implements AggregationMatchRulesSupplier {

    private static final AggregationMatchRules INSTANCE = createDefault();

    private static AggregationMatchRules createDefault() {
        var tm = new AggregationTableMatchRecord("ta", null, "agg_.+_", null, null);
        var fcm = new AggregationFactCountMatchRecord("fca", null, null, null, null, null);
        var lvlMap = new AggregationLevelMapRecord("lxx", List.of(
                new AggregationMatchRegexRecord("logical", LOWER, "${hierarchy_name}_${level_name}", null, null),
                new AggregationMatchRegexRecord("mixed", LOWER, "${hierarchy_name}_${level_column_name}", null, null),
                new AggregationMatchRegexRecord("usage", EXACT, "${usage_prefix}${level_column_name}", null, null),
                new AggregationMatchRegexRecord("physical", EXACT, "${level_column_name}", null, null)));
        var measMap = new AggregationMeasureMapRecord("mxx",
                List.of(new AggregationMatchRegexRecord("logical", LOWER, "${measure_name}", null, null),
                        new AggregationMatchRegexRecord("foreignkey", EXACT, "${measure_column_name}", null, null),
                        new AggregationMatchRegexRecord("physical", EXACT, "${measure_column_name}_${aggregate_name}",
                                null, null)));
        var fkm = new AggregationForeignKeyMatchRecord("fka", null, null, null, null);
        var defaultRule = new AggregationMatchRuleRecord("default", null, tm, fcm, fkm, lvlMap, measMap, null);
        return new AggregationMatchRulesRecord(List.of(defaultRule));
    }

    @Override
    public AggregationMatchRules get() {
        return INSTANCE;
    }
}
