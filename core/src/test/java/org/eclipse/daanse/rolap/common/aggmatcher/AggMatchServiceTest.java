/*
 * Copyright (c) 2024 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *   SmartCity Jena - initial
 *   Stefan Bischof (bipolis.org) - initial
 */
package org.eclipse.daanse.rolap.common.aggmatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase.EXACT;
import static org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase.LOWER;
import static org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase.UPPER;

import java.util.List;

import org.eclipse.daanse.rolap.aggmatch.impl.AggregationFactCountMatchRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationForeignKeyMatchRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationLevelMapRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationMatchRegexRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationMeasureMapRecord;
import org.eclipse.daanse.rolap.aggmatch.impl.AggregationTableMatchRecord;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationFactCountMatch;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationForeignKeyMatch;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationLevelMap;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMeasureMap;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationTableMatch;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AggMatchService} covering scenarios extracted from
 * disabled legacy.xmla tests:
 * <ul>
 *   <li>UsagePrefixTest — usage prefix in level matching</li>
 *   <li>PatternbasedRecognizerTest — distinct-count measure matching</li>
 *   <li>NonCollapsedAggTest — complex hierarchy names with dots</li>
 *   <li>AggMeasureFactCountTest — fact count column variations</li>
 *   <li>ExplicitRecognizerTest — custom fact count column names</li>
 * </ul>
 */
class AggMatchServiceTest {

    // ---------------------------------------------------------------
    // Helpers to create default matchers (same config as PatternbasedRules)
    // ---------------------------------------------------------------

    private static AggregationTableMatch createDefaultTableMatch() {
        return new AggregationTableMatchRecord("ta", null, "agg_.+_", null, null);
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

    // ---------------------------------------------------------------
    // 1. Usage Prefix Level Matching (from UsagePrefixTest)
    // ---------------------------------------------------------------

    @Test
    void usagePrefixMatchesCorrectColumn() {
        AggregationLevelMap lvlMap = createDefaultLevelMap();

        // usagePrefix="firstprefix_", levelColumnName="value"
        // The usage regex is: ${usage_prefix}${level_column_name} (EXACT)
        // So it should match "firstprefix_value"
        Recognizer.Matcher matcher = AggMatchService.createLevelMatcher(
            lvlMap, "firstprefix_", null, null, "value");

        assertThat(matcher.matches("firstprefix_value"))
            .as("firstprefix_value should match with usagePrefix=firstprefix_")
            .isTrue();
        assertThat(matcher.matches("secondprefix_value"))
            .as("secondprefix_value should NOT match with usagePrefix=firstprefix_")
            .isFalse();
        assertThat(matcher.matches("value"))
            .as("value should also match via physical regex")
            .isTrue();
    }

    @Test
    void multipleUsagePrefixesAreIndependent() {
        AggregationLevelMap lvlMap1 = createDefaultLevelMap();
        AggregationLevelMap lvlMap2 = createDefaultLevelMap();

        Recognizer.Matcher matcher1 = AggMatchService.createLevelMatcher(
            lvlMap1, "firstprefix_", null, null, "value");
        Recognizer.Matcher matcher2 = AggMatchService.createLevelMatcher(
            lvlMap2, "secondprefix_", null, null, "value");

        // matcher1 matches firstprefix_ but not secondprefix_
        assertThat(matcher1.matches("firstprefix_value")).isTrue();
        assertThat(matcher1.matches("secondprefix_value")).isFalse();

        // matcher2 matches secondprefix_ but not firstprefix_
        assertThat(matcher2.matches("secondprefix_value")).isTrue();
        assertThat(matcher2.matches("firstprefix_value")).isFalse();

        // both match the physical column name
        assertThat(matcher1.matches("value")).isTrue();
        assertThat(matcher2.matches("value")).isTrue();
    }

    @Test
    void nullUsagePrefixMatchesColumnDirectly() {
        AggregationLevelMap lvlMap = createDefaultLevelMap();

        // null usagePrefix: the usage regex ${usage_prefix}${level_column_name}
        // produces null for usage_prefix → that regex returns null → skipped
        // But physical regex ${level_column_name} matches "value" directly
        Recognizer.Matcher matcher = AggMatchService.createLevelMatcher(
            lvlMap, null, "Store", "Store Value", "value");

        assertThat(matcher.matches("value"))
            .as("value should match via physical regex")
            .isTrue();
        assertThat(matcher.matches("store_store_value"))
            .as("store_store_value should match via logical regex")
            .isTrue();
        assertThat(matcher.matches("firstprefix_value"))
            .as("firstprefix_value should NOT match with null usagePrefix")
            .isFalse();
    }

    // ---------------------------------------------------------------
    // 2. Table Matching (from PatternbasedRecognizerTest, NonCollapsedAggTest)
    // ---------------------------------------------------------------

    @Test
    void tableMatcherRecognizesVariousAggTableNames() {
        AggregationTableMatch tm = createDefaultTableMatch();

        // agg_g_ms_pcat_sales_fact_1997 matches fact table "sales_fact_1997"
        Recognizer.Matcher matcher1 = AggMatchService.createTableMatcher(tm, "sales_fact_1997");
        assertThat(matcher1.matches("agg_g_ms_pcat_sales_fact_1997")).isTrue();
        assertThat(matcher1.matches("agg_c_10_sales_fact_1997")).isTrue();

        // agg_lp_595_cheques matches fact table "cheques"
        Recognizer.Matcher matcher2 = AggMatchService.createTableMatcher(tm, "cheques");
        assertThat(matcher2.matches("agg_lp_595_cheques")).isTrue();
        assertThat(matcher2.matches("agg_1_cheques")).isTrue();
    }

    @Test
    void tableMatcherRejectsNonAggTables() {
        AggregationTableMatch tm = createDefaultTableMatch();

        Recognizer.Matcher matcher = AggMatchService.createTableMatcher(tm, "sales_fact_1997");
        assertThat(matcher.matches("sales_fact_1997"))
            .as("fact table itself should NOT match").isFalse();
        assertThat(matcher.matches("fact_1997_agg"))
            .as("reversed pattern should NOT match").isFalse();
        assertThat(matcher.matches("summary_sales"))
            .as("unrelated table should NOT match").isFalse();
        assertThat(matcher.matches("agg_sales_fact_1997"))
            .as("missing middle segment should NOT match").isFalse();
        assertThat(matcher.matches("agg__sales_fact_1997"))
            .as("empty middle segment should NOT match").isFalse();
    }

    // ---------------------------------------------------------------
    // 3. Measure Matching with Distinct Count (from PatternbasedRecognizerTest)
    // ---------------------------------------------------------------

    @Test
    void measureMatcherWithDistinctCount() {
        AggregationMeasureMap measMap = createDefaultMeasureMap();

        // "Customer Count" measure with column "customer_count" and aggregate "distinct-count"
        Recognizer.Matcher matcher = AggMatchService.createMeasureMatcher(
            measMap, "Customer Count", "customer_count", "distinct-count");

        // logical regex: ${measure_name} → "customer_count" (lowercase of "Customer Count")
        assertThat(matcher.matches("customer_count"))
            .as("customer_count should match via logical regex").isTrue();
        // foreignkey regex: ${measure_column_name} → "customer_count" (EXACT)
        assertThat(matcher.matches("customer_count")).isTrue();
        // physical regex: ${measure_column_name}_${aggregate_name} → "customer_count_distinct-count"
        assertThat(matcher.matches("customer_count_distinct-count"))
            .as("customer_count_distinct-count should match via physical regex").isTrue();

        assertThat(matcher.matches("Customer Count"))
            .as("original case should NOT match (logical uses LOWER)").isFalse();
    }

    @Test
    void measureMatcherWithSum() {
        AggregationMeasureMap measMap = createDefaultMeasureMap();

        Recognizer.Matcher matcher = AggMatchService.createMeasureMatcher(
            measMap, "Store Sales", "store_sales", "sum");

        assertThat(matcher.matches("store_sales"))
            .as("store_sales matches via logical or foreignkey regex").isTrue();
        assertThat(matcher.matches("store_sales_sum"))
            .as("store_sales_sum matches via physical regex").isTrue();
        assertThat(matcher.matches("Store Sales"))
            .as("original case should NOT match").isFalse();
    }

    @Test
    void measureMatcherRejectsWrongNames() {
        AggregationMeasureMap measMap = createDefaultMeasureMap();

        Recognizer.Matcher matcher = AggMatchService.createMeasureMatcher(
            measMap, "Store Sales", "store_sales", "sum");

        assertThat(matcher.matches("unit_sales")).isFalse();
        assertThat(matcher.matches("store_cost")).isFalse();
        assertThat(matcher.matches("store_sales_count")).isFalse();
        assertThat(matcher.matches("total_store_sales")).isFalse();
    }

    // ---------------------------------------------------------------
    // 4. Level Matching with Complex Hierarchy Names
    //    (from NonCollapsedAggTest, PatternbasedRecognizerTest)
    // ---------------------------------------------------------------

    @Test
    void levelMatcherWithDottedHierarchyName() {
        AggregationLevelMap lvlMap = createDefaultLevelMap();

        // hierarchy "dimension.distributor", level "line class"
        // dot replaced with "_", space replaced with "_"
        // logical: ${hierarchy_name}_${level_name} → "dimension_distributor_line_class"
        Recognizer.Matcher matcher = AggMatchService.createLevelMatcher(
            lvlMap, null, "dimension.distributor", "line class", "distributor_id");

        assertThat(matcher.matches("dimension_distributor_line_class"))
            .as("logical regex with dot→_ and space→_").isTrue();
        assertThat(matcher.matches("dimension_distributor_distributor_id"))
            .as("mixed regex with hierarchy_name and column_name").isTrue();
        assertThat(matcher.matches("distributor_id"))
            .as("physical regex matches column directly").isTrue();
    }

    @Test
    void levelMatcherWithTimeHierarchy() {
        AggregationLevelMap lvlMap = createDefaultLevelMap();

        // Time hierarchy: columns the_year, quarter, month_of_year
        Recognizer.Matcher yearMatcher = AggMatchService.createLevelMatcher(
            lvlMap, null, "Time", "Year", "the_year");
        Recognizer.Matcher quarterMatcher = AggMatchService.createLevelMatcher(
            lvlMap, null, "Time", "Quarter", "quarter");
        Recognizer.Matcher monthMatcher = AggMatchService.createLevelMatcher(
            lvlMap, null, "Time", "Month", "month_of_year");

        // Physical regex (EXACT case) matches column names directly
        assertThat(yearMatcher.matches("the_year")).isTrue();
        assertThat(quarterMatcher.matches("quarter")).isTrue();
        assertThat(monthMatcher.matches("month_of_year")).isTrue();

        // Logical regex (LOWER) matches hierarchy_level pattern
        assertThat(yearMatcher.matches("time_year")).isTrue();
        assertThat(quarterMatcher.matches("time_quarter")).isTrue();
        assertThat(monthMatcher.matches("time_month")).isTrue();

        // Mixed regex (LOWER) matches hierarchy_column pattern
        assertThat(yearMatcher.matches("time_the_year")).isTrue();

        // Cross-matches should fail
        assertThat(yearMatcher.matches("quarter")).isFalse();
        assertThat(quarterMatcher.matches("the_year")).isFalse();
    }

    @Test
    void levelMatcherByPhysicalColumnOnly() {
        AggregationLevelMap lvlMap = createDefaultLevelMap();

        // Physical regex: ${level_column_name} with EXACT case
        Recognizer.Matcher matcher = AggMatchService.createLevelMatcher(
            lvlMap, null, "Product", "Category", "product_category");

        assertThat(matcher.matches("product_category"))
            .as("exact column name match").isTrue();
        assertThat(matcher.matches("PRODUCT_CATEGORY"))
            .as("uppercase should NOT match EXACT case regex").isFalse();
        assertThat(matcher.matches("product_category_id"))
            .as("extra suffix should NOT match").isFalse();
    }

    // ---------------------------------------------------------------
    // 5. Fact Count Column Variations
    //    (from AggMeasureFactCountTest, ExplicitRecognizerTest)
    // ---------------------------------------------------------------

    @Test
    void factCountDefaultName() {
        AggregationFactCountMatch fcm = new AggregationFactCountMatchRecord(
            null, null, null, null, null, null);

        Recognizer.Matcher matcher = AggMatchService.createFactCountMatcher(fcm);

        assertThat(matcher.matches("fact_count")).isTrue();
        assertThat(matcher.matches("FACT_COUNT")).isTrue();
        assertThat(matcher.matches("Fact_Count")).isTrue();
        assertThat(matcher.matches("my_fact_count")).isFalse();
        assertThat(matcher.matches("fact_count_my")).isFalse();
    }

    @Test
    void factCountCustomName() {
        AggregationFactCountMatch fcm = new AggregationFactCountMatchRecord(
            null, null, null, null, null, "factCountExpAggTest");

        Recognizer.Matcher matcher = AggMatchService.createFactCountMatcher(fcm);

        assertThat(matcher.matches("factCountExpAggTest")).isTrue();
        assertThat(matcher.matches("FACTCOUNTAGGTEST"))
            .as("case insensitive match").isFalse(); // different name entirely
        assertThat(matcher.matches("factcountexpaggtest")).isTrue();
        assertThat(matcher.matches("FACTCOUNTEXPAGGTEST")).isTrue();
        assertThat(matcher.matches("fact_count")).isFalse();
    }

    @Test
    void factCountUpperCaseOnly() {
        AggregationFactCountMatch fcm = new AggregationFactCountMatchRecord(
            null, UPPER, null, null, null, "my_fact_count");

        Recognizer.Matcher matcher = AggMatchService.createFactCountMatcher(fcm);

        assertThat(matcher.matches("MY_FACT_COUNT"))
            .as("UPPER case: name uppercased to MY_FACT_COUNT").isTrue();
        assertThat(matcher.matches("my_fact_count"))
            .as("lowercase should NOT match UPPER case regex").isFalse();
        assertThat(matcher.matches("My_Fact_Count"))
            .as("mixed case should NOT match UPPER case regex").isFalse();
    }

    // ---------------------------------------------------------------
    // 6. Foreign Key Matching (from PatternbasedRecognizerTest)
    // ---------------------------------------------------------------

    @Test
    void foreignKeyMatcherCaseInsensitive() {
        AggregationForeignKeyMatch fkm = new AggregationForeignKeyMatchRecord(
            null, null, null, null, null);

        Recognizer.Matcher matcher = AggMatchService.createForeignKeyMatcher(fkm, "product_id");

        assertThat(matcher.matches("product_id")).isTrue();
        assertThat(matcher.matches("PRODUCT_ID")).isTrue();
        assertThat(matcher.matches("Product_Id")).isTrue();
        assertThat(matcher.matches("product_id_key")).isFalse();
        assertThat(matcher.matches("fk_product_id")).isFalse();
    }

    @Test
    void foreignKeyMatcherWithDimensionColumn() {
        AggregationForeignKeyMatch fkm = new AggregationForeignKeyMatchRecord(
            null, null, null, null, null);

        // Dimension join columns from PatternbasedRecognizerTest scenario
        Recognizer.Matcher genderMatcher = AggMatchService.createForeignKeyMatcher(fkm, "gender");
        Recognizer.Matcher maritalMatcher = AggMatchService.createForeignKeyMatcher(fkm, "marital_status");

        assertThat(genderMatcher.matches("gender")).isTrue();
        assertThat(genderMatcher.matches("GENDER")).isTrue();
        assertThat(genderMatcher.matches("gender_id")).isFalse();

        assertThat(maritalMatcher.matches("marital_status")).isTrue();
        assertThat(maritalMatcher.matches("MARITAL_STATUS")).isTrue();
        assertThat(maritalMatcher.matches("marital")).isFalse();
    }
}
