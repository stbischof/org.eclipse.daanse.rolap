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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.eclipse.daanse.rolap.aggmatch.aggmatch.AggMatchFactory;
import org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationCharacterCase;
import org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationFactCountMatch;
import org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationForeignKeyMatch;
import org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationIgnoreMap;
import org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationLevelMap;
import org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationMatchRegex;
import org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationMatchRule;
import org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationMatchRules;
import org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationMeasureMap;
import org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationTableMatch;
import org.eclipse.daanse.rolap.aggmatch.instance.basic.BasicAggMatchRulesSupplier;
import org.junit.jupiter.api.Test;

class EmfAggMatchConverterTest {

    private static final AggMatchFactory F = AggMatchFactory.eINSTANCE;

    @Test
    void convertTableMatch() {
        AggregationTableMatch tm = F.createAggregationTableMatch();
        tm.setId("ta");
        tm.setPretemplate("agg_.+_");

        AggregationMatchRules emfRules = rulesWith(r -> r.setTableMatch(tm));
        var result = EmfAggMatchConverter.convert(emfRules);

        var rule = result.getAggregationRules().get(0);
        assertThat(rule.getTableMatch().getId()).isEqualTo("ta");
        assertThat(rule.getTableMatch().getPrefixRegex()).hasValue("agg_.+_");
        assertThat(rule.getTableMatch().getSuffixRegex()).isEmpty();
        assertThat(rule.getTableMatch().getNameExtractRegex()).isEmpty();
    }

    @Test
    void convertTableMatchAllFields() {
        AggregationTableMatch tm = F.createAggregationTableMatch();
        tm.setId("tc");
        tm.setCharCase(AggregationCharacterCase.EXACT);
        tm.setPretemplate("AGG_.+_");
        tm.setPosttemplate("_SUFFIX");
        tm.setBasename("RF_(.*)_TABLE");

        AggregationMatchRules emfRules = rulesWith(r -> r.setTableMatch(tm));
        var result = EmfAggMatchConverter.convert(emfRules);

        var converted = result.getAggregationRules().get(0).getTableMatch();
        assertThat(converted.getId()).isEqualTo("tc");
        assertThat(converted.getCharCase())
                .hasValue(org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase.EXACT);
        assertThat(converted.getPrefixRegex()).hasValue("AGG_.+_");
        assertThat(converted.getSuffixRegex()).hasValue("_SUFFIX");
        assertThat(converted.getNameExtractRegex()).hasValue("RF_(.*)_TABLE");
    }

    @Test
    void convertFactCountMatch() {
        AggregationFactCountMatch fcm = F.createAggregationFactCountMatch();
        fcm.setId("fca");

        AggregationMatchRules emfRules = rulesWith(r -> r.setFactCountMatch(fcm));
        var result = EmfAggMatchConverter.convert(emfRules);

        var converted = result.getAggregationRules().get(0).getFactCountMatch();
        assertThat(converted.getId()).isEqualTo("fca");
        assertThat(converted.getFactCountName()).hasValue("fact_count");
    }

    @Test
    void convertFactCountMatchCustom() {
        AggregationFactCountMatch fcm = F.createAggregationFactCountMatch();
        fcm.setId("fcc");
        fcm.setCharCase(AggregationCharacterCase.UPPER);
        fcm.setFactCountName("my_fact_count");

        AggregationMatchRules emfRules = rulesWith(r -> r.setFactCountMatch(fcm));
        var result = EmfAggMatchConverter.convert(emfRules);

        var converted = result.getAggregationRules().get(0).getFactCountMatch();
        assertThat(converted.getId()).isEqualTo("fcc");
        assertThat(converted.getCharCase())
                .hasValue(org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase.UPPER);
        assertThat(converted.getFactCountName()).hasValue("my_fact_count");
    }

    @Test
    void convertForeignKeyMatch() {
        AggregationForeignKeyMatch fkm = F.createAggregationForeignKeyMatch();
        fkm.setId("fka");

        AggregationMatchRules emfRules = rulesWith(r -> r.setForeignKeyMatch(fkm));
        var result = EmfAggMatchConverter.convert(emfRules);

        var converted = result.getAggregationRules().get(0).getForeignKeyMatch();
        assertThat(converted.getId()).isEqualTo("fka");
        assertThat(converted.getPrefixRegex()).isEmpty();
        assertThat(converted.getSuffixRegex()).isEmpty();
    }

    @Test
    void convertForeignKeyMatchAllFields() {
        AggregationForeignKeyMatch fkm = F.createAggregationForeignKeyMatch();
        fkm.setId("fkb");
        fkm.setCharCase(AggregationCharacterCase.UPPER);
        fkm.setPosttemplate("_FK");
        fkm.setBasename("FK_(.*)");

        AggregationMatchRules emfRules = rulesWith(r -> r.setForeignKeyMatch(fkm));
        var result = EmfAggMatchConverter.convert(emfRules);

        var converted = result.getAggregationRules().get(0).getForeignKeyMatch();
        assertThat(converted.getId()).isEqualTo("fkb");
        assertThat(converted.getCharCase())
                .hasValue(org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase.UPPER);
        assertThat(converted.getSuffixRegex()).hasValue("_FK");
        assertThat(converted.getNameExtractRegex()).hasValue("FK_(.*)");
    }

    @Test
    void convertLevelMapWithRegexes() {
        AggregationLevelMap lm = F.createAggregationLevelMap();
        lm.setId("lxx");
        lm.getRegexes()
                .addAll(List.of(regex("logical", AggregationCharacterCase.LOWER, "${hierarchy_name}_${level_name}"),
                        regex("mixed", AggregationCharacterCase.LOWER, "${hierarchy_name}_${level_column_name}"),
                        regex("usage", AggregationCharacterCase.EXACT, "${usage_prefix}${level_column_name}"),
                        regex("physical", AggregationCharacterCase.EXACT, "${level_column_name}")));

        AggregationMatchRules emfRules = rulesWith(r -> r.setLevelMap(lm));
        var result = EmfAggMatchConverter.convert(emfRules);

        var converted = result.getAggregationRules().get(0).getLevelMap();
        assertThat(converted.getId()).isEqualTo("lxx");
        assertThat(converted.getRegexes()).hasSize(4);
        assertThat(converted.getRegexes().get(0).getId()).isEqualTo("logical");
        assertThat(converted.getRegexes().get(0).getTemplate()).isEqualTo("${hierarchy_name}_${level_name}");
        assertThat(converted.getRegexes().get(0).getCharCase())
                .hasValue(org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase.LOWER);
        assertThat(converted.getRegexes().get(3).getId()).isEqualTo("physical");
        assertThat(converted.getRegexes().get(3).getTemplate()).isEqualTo("${level_column_name}");
    }

    @Test
    void convertMeasureMapWithRegexes() {
        AggregationMeasureMap mm = F.createAggregationMeasureMap();
        mm.setId("mxx");
        mm.getRegexes()
                .addAll(List.of(regex("logical", AggregationCharacterCase.LOWER, "${measure_name}"),
                        regex("foreignkey", AggregationCharacterCase.EXACT, "${measure_column_name}"),
                        regex("physical", AggregationCharacterCase.EXACT, "${measure_column_name}_${aggregate_name}")));

        AggregationMatchRules emfRules = rulesWith(r -> r.setMeasureMap(mm));
        var result = EmfAggMatchConverter.convert(emfRules);

        var converted = result.getAggregationRules().get(0).getMeasureMap();
        assertThat(converted.getId()).isEqualTo("mxx");
        assertThat(converted.getRegexes()).hasSize(3);
        assertThat(converted.getRegexes().get(0).getId()).isEqualTo("logical");
        assertThat(converted.getRegexes().get(2).getTemplate()).isEqualTo("${measure_column_name}_${aggregate_name}");
    }

    @Test
    void convertIgnoreMap() {
        AggregationIgnoreMap im = F.createAggregationIgnoreMap();
        im.setId("ign");
        im.getRegexes().add(regex("skip", AggregationCharacterCase.IGNORE, "tmp_.*"));

        AggregationMatchRules emfRules = rulesWith(r -> r.setIgnoreMap(im));
        var result = EmfAggMatchConverter.convert(emfRules);

        var converted = result.getAggregationRules().get(0).getIgnoreMap();
        assertThat(converted).isPresent();
        assertThat(converted.get().getId()).isEqualTo("ign");
        assertThat(converted.get().getRegexes()).hasSize(1);
        assertThat(converted.get().getRegexes().get(0).getTemplate()).isEqualTo("tmp_.*");
    }

    @Test
    void convertIgnoreMapEmpty() {
        AggregationIgnoreMap im = F.createAggregationIgnoreMap();
        im.setId("empty");

        AggregationMatchRules emfRules = rulesWith(r -> r.setIgnoreMap(im));
        var result = EmfAggMatchConverter.convert(emfRules);

        var converted = result.getAggregationRules().get(0).getIgnoreMap();
        assertThat(converted).isPresent();
        assertThat(converted.get().getRegexes()).isEmpty();
    }

    @Test
    void convertNoIgnoreMap() {
        // ignoreMap is the only optional sub-element; verify it converts to empty
        // Optional
        AggregationMatchRules emfRules = rulesWith(r -> {
            // no ignoreMap set
        });
        var result = EmfAggMatchConverter.convert(emfRules);
        assertThat(result.getAggregationRules().get(0).getIgnoreMap()).isEmpty();
    }

    @Test
    void convertRuleEnabled() {
        AggregationMatchRules emfRules = rulesWith(r -> r.setEnabled(true));
        var result = EmfAggMatchConverter.convert(emfRules);
        assertThat(result.getAggregationRules().get(0).getEnabled()).hasValue(true);
    }

    @Test
    void convertAllCharCaseValues() {
        for (AggregationCharacterCase emfCase : AggregationCharacterCase.values()) {
            AggregationTableMatch tm = F.createAggregationTableMatch();
            tm.setId("test");
            tm.setCharCase(emfCase);

            AggregationMatchRules emfRules = rulesWith(r -> r.setTableMatch(tm));
            var result = EmfAggMatchConverter.convert(emfRules);

            var converted = result.getAggregationRules().get(0).getTableMatch();
            assertThat(converted.getCharCase()).as("EMF %s should map to API %s", emfCase.name(), emfCase.name())
                    .hasValue(org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase.valueOf(emfCase.name()));
        }
    }

    @Test
    void convertRegexCustomSpaceDot() {
        AggregationMatchRegex regex = F.createAggregationMatchRegex();
        regex.setId("custom");
        regex.setTemplate("${measure_name}");
        regex.setSpace("_SP_");
        regex.setDot("_DOT_");

        AggregationMeasureMap mm = F.createAggregationMeasureMap();
        mm.setId("mm");
        mm.getRegexes().add(regex);

        AggregationMatchRules emfRules = rulesWith(r -> r.setMeasureMap(mm));
        var result = EmfAggMatchConverter.convert(emfRules);

        var converted = result.getAggregationRules().get(0).getMeasureMap().getRegexes().get(0);
        assertThat(converted.getSpace()).hasValue("_SP_");
        assertThat(converted.getDot()).hasValue("_DOT_");
    }

    @Test
    void convertDefaultRules() {
        AggregationMatchRules emfRules = buildDefaultEmfRules();
        var converted = EmfAggMatchConverter.convert(emfRules);
        var expected = new BasicAggMatchRulesSupplier().get();

        assertThat(converted.getAggregationRules()).hasSize(1);
        var cRule = converted.getAggregationRules().get(0);
        var eRule = expected.getAggregationRules().get(0);

        assertThat(cRule.getTag()).isEqualTo(eRule.getTag());

        // TableMatch
        assertThat(cRule.getTableMatch().getId()).isEqualTo(eRule.getTableMatch().getId());
        assertThat(cRule.getTableMatch().getPrefixRegex()).isEqualTo(eRule.getTableMatch().getPrefixRegex());

        // FactCountMatch
        assertThat(cRule.getFactCountMatch().getId()).isEqualTo(eRule.getFactCountMatch().getId());

        // ForeignKeyMatch
        assertThat(cRule.getForeignKeyMatch().getId()).isEqualTo(eRule.getForeignKeyMatch().getId());

        // LevelMap
        assertThat(cRule.getLevelMap().getId()).isEqualTo(eRule.getLevelMap().getId());
        assertThat(cRule.getLevelMap().getRegexes()).hasSameSizeAs(eRule.getLevelMap().getRegexes());
        for (int i = 0; i < cRule.getLevelMap().getRegexes().size(); i++) {
            var cr = cRule.getLevelMap().getRegexes().get(i);
            var er = eRule.getLevelMap().getRegexes().get(i);
            assertThat(cr.getId()).isEqualTo(er.getId());
            assertThat(cr.getTemplate()).isEqualTo(er.getTemplate());
        }

        // MeasureMap
        assertThat(cRule.getMeasureMap().getId()).isEqualTo(eRule.getMeasureMap().getId());
        assertThat(cRule.getMeasureMap().getRegexes()).hasSameSizeAs(eRule.getMeasureMap().getRegexes());
        for (int i = 0; i < cRule.getMeasureMap().getRegexes().size(); i++) {
            var cr = cRule.getMeasureMap().getRegexes().get(i);
            var er = eRule.getMeasureMap().getRegexes().get(i);
            assertThat(cr.getId()).isEqualTo(er.getId());
            assertThat(cr.getTemplate()).isEqualTo(er.getTemplate());
        }

        // IgnoreMap
        assertThat(cRule.getIgnoreMap()).isEmpty();
        assertThat(eRule.getIgnoreMap()).isEmpty();
    }

    // --- helpers ---

    private static AggregationMatchRegex regex(String id, AggregationCharacterCase charCase, String template) {
        AggregationMatchRegex r = F.createAggregationMatchRegex();
        r.setId(id);
        r.setCharCase(charCase);
        r.setTemplate(template);
        return r;
    }

    private static AggregationMatchRules rulesWith(java.util.function.Consumer<AggregationMatchRule> configurator) {
        AggregationMatchRules rules = F.createAggregationMatchRules();
        rules.setTag("test");
        AggregationMatchRule rule = F.createAggregationMatchRule();
        rule.setTag("test-rule");

        // provide required defaults so the record constructor doesn't throw NPE
        AggregationTableMatch dtm = F.createAggregationTableMatch();
        dtm.setId("stub-tm");
        rule.setTableMatch(dtm);
        AggregationFactCountMatch dfcm = F.createAggregationFactCountMatch();
        dfcm.setId("stub-fcm");
        rule.setFactCountMatch(dfcm);
        AggregationForeignKeyMatch dfkm = F.createAggregationForeignKeyMatch();
        dfkm.setId("stub-fkm");
        rule.setForeignKeyMatch(dfkm);
        AggregationLevelMap dlm = F.createAggregationLevelMap();
        dlm.setId("stub-lm");
        rule.setLevelMap(dlm);
        AggregationMeasureMap dmm = F.createAggregationMeasureMap();
        dmm.setId("stub-mm");
        rule.setMeasureMap(dmm);

        // let the test override whichever field it wants
        configurator.accept(rule);
        rules.getAggregationRules().add(rule);
        return rules;
    }

    private static AggregationMatchRules buildDefaultEmfRules() {
        AggregationTableMatch tm = F.createAggregationTableMatch();
        tm.setId("ta");
        tm.setPretemplate("agg_.+_");

        AggregationFactCountMatch fcm = F.createAggregationFactCountMatch();
        fcm.setId("fca");

        AggregationForeignKeyMatch fkm = F.createAggregationForeignKeyMatch();
        fkm.setId("fka");

        AggregationLevelMap lm = F.createAggregationLevelMap();
        lm.setId("lxx");
        lm.getRegexes()
                .addAll(List.of(regex("logical", AggregationCharacterCase.LOWER, "${hierarchy_name}_${level_name}"),
                        regex("mixed", AggregationCharacterCase.LOWER, "${hierarchy_name}_${level_column_name}"),
                        regex("usage", AggregationCharacterCase.EXACT, "${usage_prefix}${level_column_name}"),
                        regex("physical", AggregationCharacterCase.EXACT, "${level_column_name}")));

        AggregationMeasureMap mm = F.createAggregationMeasureMap();
        mm.setId("mxx");
        mm.getRegexes()
                .addAll(List.of(regex("logical", AggregationCharacterCase.LOWER, "${measure_name}"),
                        regex("foreignkey", AggregationCharacterCase.EXACT, "${measure_column_name}"),
                        regex("physical", AggregationCharacterCase.EXACT, "${measure_column_name}_${aggregate_name}")));

        AggregationMatchRule rule = F.createAggregationMatchRule();
        rule.setTag("default");
        rule.setTableMatch(tm);
        rule.setFactCountMatch(fcm);
        rule.setForeignKeyMatch(fkm);
        rule.setLevelMap(lm);
        rule.setMeasureMap(mm);

        AggregationMatchRules rules = F.createAggregationMatchRules();
        rules.setTag("default-config");
        rules.getAggregationRules().add(rule);
        return rules;
    }
}
