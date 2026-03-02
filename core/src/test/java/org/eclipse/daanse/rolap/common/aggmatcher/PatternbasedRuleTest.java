/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
 *
 * ---- All changes after Fork in 2023 ------------------------
 *
 * Project: Eclipse daanse
 *
 * Copyright (c) 2023 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors after Fork in 2023:
 *   SmartCity Jena - initial
 */

package org.eclipse.daanse.rolap.common.aggmatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase.EXACT;
import static org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase.LOWER;
import static org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase.UPPER;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.daanse.rolap.aggmatch.impl.AggregationFactCountMatchRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationForeignKeyMatchRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationLevelMapRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationMatchRegexRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationMatchRuleRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationMeasureMapRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationTableMatchRecord;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationFactCountMatch;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationForeignKeyMatch;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationLevelMap;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRegex;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRule;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMeasureMap;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationTableMatch;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing the default aggregate table recognizer.
 *
 * @author Richard M. Emberson
 */
class PatternbasedRuleTest {
    private static final Logger LOGGER =
        LoggerFactory.getLogger(PatternbasedRuleTest.class);

    private static Map<String, AggregationMatchRule> rules;

    private AggregationMatchRule getAggRule(String tag) {
        return rules.get(tag);
    }

    @BeforeAll
    static void beforeAll() {
        rules = prepareRules();
    }

    private static Map<String, AggregationMatchRule> prepareRules() {
        Map<String, AggregationMatchRule> ruleMap = new HashMap<>();

        // --- "default" rule ---
        ruleMap.put("default", new AggregationMatchRuleRecord(
            "default", null,
            new AggregationTableMatchRecord("ta", null, "agg_.+_", null, null),
            new AggregationFactCountMatchRecord("fca", null, null, null, null, null),
            new AggregationForeignKeyMatchRecord("fka", null, null, null, null),
            createDefaultLevelMap(),
            createDefaultMeasureMap(),
            null
        ));

        // --- "bbbb" rule ---
        ruleMap.put("bbbb", new AggregationMatchRuleRecord(
            "bbbb", null,
            new AggregationTableMatchRecord("tb", null, null, "_agg_.+", null),
            new AggregationFactCountMatchRecord("fcb", null, null, null, null, "my_fact_count"),
            new AggregationForeignKeyMatchRecord("fkb", UPPER, null, "_FK", "FK_(.*)"),
            new AggregationLevelMapRecord("lb", List.of(
                new AggregationMatchRegexRecord("usage", LOWER,
                    "${usage_prefix}${hierarchy_name}_${level_name}_${level_column_name}",
                    "_SP_", "_DOT_")
            )),
            createDefaultMeasureMap(),
            null
        ));

        // --- "cccc" rule ---
        ruleMap.put("cccc", new AggregationMatchRuleRecord(
            "cccc", null,
            new AggregationTableMatchRecord("tc", null, "AGG_.+_", null, "RF_(.*)_TABLE"),
            new AggregationFactCountMatchRecord("fcc", UPPER, null, null, null, "my_fact_count"),
            new AggregationForeignKeyMatchRecord("fkc", EXACT, null, "_[fF][kK]", "(?:FK|fk)_(.*)"),
            createDefaultLevelMap(),
            createDefaultMeasureMap(),
            null
        ));

        return ruleMap;
    }

    private static AggregationLevelMap createDefaultLevelMap() {
        return new AggregationLevelMapRecord("lxx", List.of(
            new AggregationMatchRegexRecord("logical",  LOWER, "${hierarchy_name}_${level_name}",        null, null),
            new AggregationMatchRegexRecord("mixed",    LOWER, "${hierarchy_name}_${level_column_name}",  null, null),
            new AggregationMatchRegexRecord("usage",    EXACT, "${usage_prefix}${level_column_name}",     null, null),
            new AggregationMatchRegexRecord("physical", EXACT, "${level_column_name}",                    null, null)
        ));
    }

    private static AggregationMeasureMap createDefaultMeasureMap() {
        return new AggregationMeasureMapRecord("mxx", List.of(
            new AggregationMatchRegexRecord("logical",    LOWER, "${measure_name}",                          null, null),
            new AggregationMatchRegexRecord("foreignkey", EXACT, "${measure_column_name}",                   null, null),
            new AggregationMatchRegexRecord("physical",   EXACT, "${measure_column_name}_${aggregate_name}", null, null)
        ));
    }

    private Recognizer.Matcher getTableMatcher(String tag, String tableName) {
        AggregationMatchRule rule = getAggRule(tag);
        if (rule == null) {
            LOGGER.info("rule == null for tag={}", tag);
        }
        AggregationTableMatch tableMatch = rule.getTableMatch();
        if (tableMatch == null) {
            LOGGER.info("tableMatch == null for tag={}, tableName={}", tag, tableName);
        }
        return AggMatchService.createTableMatcher(tableMatch, tableName);
    }

    private Recognizer.Matcher getFactCountMatcher(String tag) {
        AggregationMatchRule rule = getAggRule(tag);
        AggregationFactCountMatch factCountMatch = rule.getFactCountMatch();
        return AggMatchService.createFactCountMatcher(factCountMatch);
    }

    private Recognizer.Matcher getForeignKeyMatcher(String tag, String foreignKeyName) {
        AggregationMatchRule rule = getAggRule(tag);
        AggregationForeignKeyMatch foreignKeyMatch = rule.getForeignKeyMatch();
        return AggMatchService.createForeignKeyMatcher(foreignKeyMatch, foreignKeyName);
    }

    private Recognizer.Matcher getLevelMatcher(
        String tag,
        String usagePrefix,
        String hierarchyName,
        String levelName,
        String levelColumnName)
    {
        AggregationMatchRule rule = getAggRule(tag);
        return AggMatchService.createLevelMatcher(
            rule.getLevelMap(),
            usagePrefix,
            hierarchyName,
            levelName,
            levelColumnName);
    }

    private Recognizer.Matcher getMeasureMatcher(
        String tag,
        String measureName,
        String measureColumnName,
        String aggregateName)
    {
        AggregationMatchRule rule = getAggRule(tag);
        return AggMatchService.createMeasureMatcher(
            rule.getMeasureMap(),
            measureName,
            measureColumnName,
            aggregateName);
    }

    //////////////////////////////////////////////////////////////////////////
    //
    // tests
    //
    //

    @Test
    void tableNameDefault() {
        final String tag = "default";
        final String factTableName = "FACT_TABLE";

        Recognizer.Matcher matcher = getTableMatcher(tag, factTableName);

        doMatch(matcher, "agg_10_" + factTableName);
        doMatch(matcher, "AGG_10_" + factTableName);
        doMatch(matcher, "agg_this_is_ok_" + factTableName);
        doMatch(matcher, "AGG_THIS_IS_OK_" + factTableName);
        doMatch(matcher, "agg_10_" + factTableName.toLowerCase());
        doMatch(matcher, "AGG_10_" + factTableName.toLowerCase());
        doMatch(matcher, "agg_this_is_ok_" + factTableName.toLowerCase());
        doMatch(matcher, "AGG_THIS_IS_OK_" + factTableName.toLowerCase());

        doNotMatch(matcher, factTableName);
        doNotMatch(matcher, "agg__" + factTableName);
        doNotMatch(matcher, "agg_" + factTableName);
        doNotMatch(matcher, factTableName + "_agg");
        doNotMatch(matcher, "agg_10_Mytable");
    }

    @Test
    void tableNameBBBB() {
        final String tag = "bbbb";
        final String factTableName = "FACT_TABLE";

        Recognizer.Matcher matcher = getTableMatcher(tag, factTableName);

        doMatch(matcher, factTableName + "_agg_10");
        doMatch(matcher, factTableName + "_agg_this_is_ok");

        doNotMatch(matcher, factTableName);
        doNotMatch(matcher, factTableName + "_agg");
        doNotMatch(matcher, factTableName + "__agg");
        doNotMatch(matcher, "agg_" + factTableName);
        doNotMatch(matcher, "Mytable_agg_10");
    }

    @Test
    void tableNameCCCCBAD() {
        final String tag = "cccc";
        final String basename = "WAREHOUSE";

        // Note that the "basename" and not the fact table name is
        // being used. The Matcher that is return will not match anything
        // because the basename does not match the table basename pattern.
        Recognizer.Matcher matcher = getTableMatcher(tag, basename);

        doNotMatch(matcher, "AGG_10_" + basename);
        doNotMatch(matcher, "agg_this_is_ok_" + basename);

        doNotMatch(matcher, "RF_" + basename + "_TABLE");
        doNotMatch(matcher, "agg__" + basename);
        doNotMatch(matcher, "agg_" + basename);
        doNotMatch(matcher, basename + "_agg");
        doNotMatch(matcher, "agg_10_Mytable");
    }

    @Test
    void tableNameCCCCGOOD() {
        final String tag = "cccc";
        final String basename = "WAREHOUSE";
        final String factTableName = "RF_" + basename + "_TABLE";

        Recognizer.Matcher matcher = getTableMatcher(tag, factTableName);

        doMatch(matcher, "AGG_10_" + basename);
        doMatch(matcher, "agg_this_is_ok_" + basename);

        doNotMatch(matcher, factTableName);
        doNotMatch(matcher, "agg__" + basename);
        doNotMatch(matcher, "agg_" + basename);
        doNotMatch(matcher, basename + "_agg");
        doNotMatch(matcher, "agg_10_Mytable");
    }

    @Test
    void factCountDefault() {
        final String tag = "default";
        Recognizer.Matcher matcher = getFactCountMatcher(tag);

        doMatch(matcher, "fact_count");
        doMatch(matcher, "FACT_COUNT");

        doNotMatch(matcher, "my_fact_count");
        doNotMatch(matcher, "MY_FACT_COUNT");
        doNotMatch(matcher, "count");
        doNotMatch(matcher, "COUNT");
        doNotMatch(matcher, "fact_count_my");
        doNotMatch(matcher, "FACT_COUNT_MY");
    }

    @Test
    void factCountBBBB() {
        final String tag = "bbbb";
        Recognizer.Matcher matcher = getFactCountMatcher(tag);

        doMatch(matcher, "my_fact_count");
        doMatch(matcher, "MY_FACT_COUNT");

        doNotMatch(matcher, "fact_count");
        doNotMatch(matcher, "FACT_COUNT");
        doNotMatch(matcher, "count");
        doNotMatch(matcher, "COUNT");
        doNotMatch(matcher, "fact_count_my");
        doNotMatch(matcher, "FACT_COUNT_MY");
    }

    @Test
    void factCountCCCC() {
        final String tag = "cccc";
        Recognizer.Matcher matcher = getFactCountMatcher(tag);

        doMatch(matcher, "MY_FACT_COUNT");

        doNotMatch(matcher, "my_fact_count");
        doNotMatch(matcher, "fact_count");
        doNotMatch(matcher, "FACT_COUNT");
        doNotMatch(matcher, "count");
        doNotMatch(matcher, "COUNT");
        doNotMatch(matcher, "fact_count_my");
        doNotMatch(matcher, "FACT_COUNT_MY");
    }

    @Test
    void foreignKeyDefault() {
        final String tag = "default";
        final String foreignKeyName = "foo_key";
        Recognizer.Matcher matcher = getForeignKeyMatcher(tag, foreignKeyName);

        doMatch(matcher, "foo_key");
        doMatch(matcher, "FOO_KEY");

        doNotMatch(matcher, "foo_key_my");
        doNotMatch(matcher, "my_foo_key");
    }

    @Test
    void foreignKeyBBBB() {
        final String tag = "bbbb";
        final String foreignKeyName = "fk_ham_n_eggs";
        Recognizer.Matcher matcher = getForeignKeyMatcher(tag, foreignKeyName);

        doMatch(matcher, "HAM_N_EGGS_FK");

        doNotMatch(matcher, "ham_n_eggs_fk");
        doNotMatch(matcher, "ham_n_eggs");
        doNotMatch(matcher, "fk_ham_n_eggs");
        doNotMatch(matcher, "HAM_N_EGGS");
        doNotMatch(matcher, "FK_HAM_N_EGGS");
    }

    @Test
    void foreignKeyCCCC() {
        final String tag = "cccc";
        final String foreignKeyName1 = "fk_toast";
        final String foreignKeyName2 = "FK_TOAST";
        final String foreignKeyName3 = "FK_ToAsT";
        Recognizer.Matcher matcher1 =
            getForeignKeyMatcher(tag, foreignKeyName1);
        Recognizer.Matcher matcher2 =
            getForeignKeyMatcher(tag, foreignKeyName2);
        Recognizer.Matcher matcher3 =
            getForeignKeyMatcher(tag, foreignKeyName3);

        doMatch(matcher1, "toast_fk");
        doNotMatch(matcher1, "TOAST_FK");

        doMatch(matcher2, "TOAST_FK");
        doNotMatch(matcher2, "toast_fk");

        doMatch(matcher3, "ToAsT_FK");
        doMatch(matcher3, "ToAsT_fk");
        doMatch(matcher3, "ToAsT_Fk");
        doNotMatch(matcher3, "toast_fk");
        doNotMatch(matcher3, "TOAST_FK");
    }

    @Test
    void levelDefaultOne() {
        final String tag = "default";
        final String usagePrefix = null;
        final String hierarchyName = "Time";
        final String levelName = "Day in Year";
        final String levelColumnName = "days";
        Recognizer.Matcher matcher = getLevelMatcher(
            tag, usagePrefix, hierarchyName, levelName, levelColumnName);

        doMatch(matcher, "days");
        doMatch(matcher, "time_day_in_year");
        doMatch(matcher, "time_days");

        doNotMatch(matcher, "DAYS");
        doNotMatch(matcher, "Time Day in Year");
    }

    @Test
    void levelDefaultTwo() {
        final String tag = "default";
        final String usagePrefix = "boo_";
        final String hierarchyName = "Time";
        final String levelName = "Day in Year";
        final String levelColumnName = "days";
        Recognizer.Matcher matcher = getLevelMatcher(
            tag, usagePrefix, hierarchyName, levelName, levelColumnName);

        doMatch(matcher, "days");
        doMatch(matcher, "boo_days");
        doMatch(matcher, "time_day_in_year");
        doMatch(matcher, "time_days");

        doNotMatch(matcher, "boo_time_day_in_year");
        doNotMatch(matcher, "boo_time_days");
        doNotMatch(matcher, "DAYS");
        doNotMatch(matcher, "Time Day in Year");
    }

    @Test
    void levelBBBB() {
        final String tag = "bbbb";
        final String usagePrefix = "boo_";
        final String hierarchyName = "Time.Period";
        final String levelName = "Day in Year";
        final String levelColumnName = "days";
        Recognizer.Matcher matcher = getLevelMatcher(
            tag, usagePrefix, hierarchyName, levelName, levelColumnName);

        doMatch(matcher, "boo_time_DOT_period_day_SP_in_SP_year_days");
    }

    @Test
    void measureDefault() {
        final String tag = "default";
        final String measureName = "Total Sales";
        final String measureColumnName = "sales";
        final String aggregateName = "sum";
        Recognizer.Matcher matcher = getMeasureMatcher(
            tag, measureName, measureColumnName, aggregateName);

        doMatch(matcher, "total_sales");
        doMatch(matcher, "sales");
        doMatch(matcher, "sales_sum");

        doNotMatch(matcher, "Total Sales");
        doNotMatch(matcher, "Total_Sales");
        doNotMatch(matcher, "total_sales_sum");
    }

    //
    //////////////////////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////////////////////
    //
    // helpers
    //
    //
    private void doMatch(Recognizer.Matcher matcher, String s) {
        assertThat(matcher.matches(s)).as("Recognizer.Matcher: " + s).isTrue();
    }

    private void doNotMatch(Recognizer.Matcher matcher, String s) {
        assertThat(matcher.matches(s)).as("Recognizer.Matcher: " + s).isFalse();
    }
    //
    //////////////////////////////////////////////////////////////////////////
}
