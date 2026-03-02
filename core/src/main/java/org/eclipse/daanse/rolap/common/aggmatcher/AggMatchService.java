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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.eclipse.daanse.rolap.api.aggmatch.AggregationCharacterCase;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationFactCountMatch;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationForeignKeyMatch;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationIgnoreMap;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationLevelMap;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchNameMatcher;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRegex;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRegexMapper;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMeasureMap;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationTableMatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides runtime matching logic for aggregate table recognition.
 * Static utility methods that compile regex patterns from aggmatch
 * model interfaces (NameMatcher, RegexMapper, Regex, etc.).
 */
public class AggMatchService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AggMatchService.class);

    private static final List<String> LEVEL_TEMPLATE_NAMES = List.of(
        "usage_prefix", "hierarchy_name", "level_name", "level_column_name");

    private static final List<String> MEASURE_TEMPLATE_NAMES = List.of(
        "measure_name", "measure_column_name", "aggregate_name");

    private static final List<String> IGNORE_TEMPLATE_NAMES = List.of();

    private static final Recognizer.Matcher NEVER_MATCHES = name -> false;

    private record ParsedTemplate(List<String> parts, List<Integer> namePositions) {}

    // --- Public API ---

    public static Recognizer.Matcher createTableMatcher(
            AggregationTableMatch tableMatch, String tableName) {
        return createNameMatcher(tableMatch, tableName);
    }

    public static Recognizer.Matcher createFactCountMatcher(
            AggregationFactCountMatch factCountMatch) {
        String factCountName = factCountMatch.getFactCountName().orElse("fact_count");
        return createNameMatcher(factCountMatch, factCountName);
    }

    public static Recognizer.Matcher createForeignKeyMatcher(
            AggregationForeignKeyMatch foreignKeyMatch, String foreignKeyName) {
        return createNameMatcher(foreignKeyMatch, foreignKeyName);
    }

    public static Recognizer.Matcher createLevelMatcher(
            AggregationLevelMap levelMap,
            String usagePrefix, String hierarchyName,
            String levelName, String levelColumnName) {
        return createRegexMapperMatcher(levelMap, LEVEL_TEMPLATE_NAMES,
            new String[]{usagePrefix, hierarchyName, levelName, levelColumnName});
    }

    public static Recognizer.Matcher createMeasureMatcher(
            AggregationMeasureMap measureMap,
            String measureName, String measureColumnName, String aggregateName) {
        return createRegexMapperMatcher(measureMap, MEASURE_TEMPLATE_NAMES,
            new String[]{measureName, measureColumnName, aggregateName});
    }

    public static Recognizer.Matcher createIgnoreMatcher(
            AggregationIgnoreMap ignoreMap) {
        return createRegexMapperMatcher(ignoreMap, IGNORE_TEMPLATE_NAMES, new String[]{});
    }

    // --- NameMatcher logic (from NameMatcher.getMatcher) ---

    static Recognizer.Matcher createNameMatcher(
            AggregationMatchNameMatcher matcher, String name) {

        AggregationCharacterCase charCase = matcher.getCharCase()
            .orElse(AggregationCharacterCase.IGNORE);

        String transformedName = switch (charCase) {
            case UPPER -> name.toUpperCase();
            case LOWER -> name.toLowerCase();
            default -> name;
        };
        int flag = charCase == AggregationCharacterCase.IGNORE ? Pattern.CASE_INSENSITIVE : 0;
        final String regex = buildNameRegex(matcher, transformedName);

        if (regex == null) {
            return NEVER_MATCHES;
        }

        final Pattern pattern = Pattern.compile(regex, flag);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("NameMatcher: compiled pattern \"{}\" for name \"{}\"", pattern.pattern(), name);
        }
        return candidateName -> pattern.matcher(candidateName).matches();
    }

    // --- NameMatcher.getRegex logic ---

    private static String buildNameRegex(AggregationMatchNameMatcher matcher, String name) {
        StringBuilder buf = new StringBuilder();

        matcher.getPrefixRegex().ifPresent(buf::append);

        if (name != null) {
            String n = name;
            var nameExtractRegex = matcher.getNameExtractRegex();
            if (nameExtractRegex.isPresent()) {
                Pattern baseNamePattern = Pattern.compile(nameExtractRegex.get());
                java.util.regex.Matcher m = baseNamePattern.matcher(name);
                if (m.matches() && m.groupCount() > 0) {
                    n = m.group(1);
                } else {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("NameMatcher.getRegex: for name \"{}\" regex is null "
                            + "because nameExtractRegex \"{}\" is not matched.", name, nameExtractRegex.get());
                    }
                    return null;
                }
            }
            buf.append(n);
        }

        matcher.getSuffixRegex().ifPresent(buf::append);

        return buf.toString();
    }

    // --- RegexMapper logic (from RegexMapper.getMatcher) ---

    static Recognizer.Matcher createRegexMapperMatcher(
            AggregationMatchRegexMapper mapper,
            List<String> templateNames,
            String[] names) {

        final List<Pattern> patterns = new ArrayList<>();

        for (AggregationMatchRegex regex : mapper.getRegexes()) {
            patterns.add(compileRegexPattern(regex, templateNames, names));
        }

        return candidateName -> {
            for (Pattern pattern : patterns) {
                if (pattern != null && pattern.matcher(candidateName).matches()) {
                    return true;
                }
            }
            return false;
        };
    }

    // --- Regex.getPattern logic ---

    private static Pattern compileRegexPattern(
            AggregationMatchRegex regex,
            List<String> templateNames,
            String[] names) {

        AggregationCharacterCase charCase = regex.getCharCase()
            .orElse(AggregationCharacterCase.IGNORE);

        String[] transformedNames = switch (charCase) {
            case UPPER -> Arrays.stream(names)
                .map(s -> s != null ? s.toUpperCase() : null)
                .toArray(String[]::new);
            case LOWER -> Arrays.stream(names)
                .map(s -> s != null ? s.toLowerCase() : null)
                .toArray(String[]::new);
            default -> names;
        };
        int flag = charCase == AggregationCharacterCase.IGNORE ? Pattern.CASE_INSENSITIVE : 0;
        String regexStr = buildTemplateRegex(regex, templateNames, transformedNames);
        return regexStr != null ? Pattern.compile(regexStr, flag) : null;
    }

    // --- Regex.getRegex + Regex.validate combined ---

    private static String buildTemplateRegex(
            AggregationMatchRegex regex,
            List<String> templateNames,
            String[] names) {

        ParsedTemplate parsed = parseTemplate(regex.getTemplate(), templateNames);
        if (parsed == null) {
            return null;
        }

        String space = regex.getSpace().orElse("_");
        String dot = regex.getDot().orElse("_");

        StringBuilder buf = new StringBuilder();
        buf.append(parsed.parts().get(0));

        for (int i = 0; i < parsed.namePositions().size(); i++) {
            int pos = parsed.namePositions().get(i);
            String n = names[pos];
            if (n == null) {
                return null;
            }

            n = n.replace(" ", space);
            n = n.replace(".", dot);

            buf.append(n);
            buf.append(parsed.parts().get(i + 1));
        }

        return buf.toString();
    }

    // --- Regex.validate template parsing ---

    private static ParsedTemplate parseTemplate(String template, List<String> templateNames) {
        if (template == null) {
            return null;
        }

        List<String> parts = new ArrayList<>();
        List<Integer> namePositions = new ArrayList<>();

        int end = 0;
        int previousEnd = 0;
        int start = template.indexOf("${", end);

        if (templateNames.isEmpty()) {
            if (start == -1) {
                parts.add(template);
                return new ParsedTemplate(parts, namePositions);
            }
            LOGGER.warn("Template \"{}\" contains ${{}} entries but no template names are defined", template);
            return null;
        }

        if (start == -1) {
            LOGGER.warn("Template \"{}\" has no ${{}} entries", template);
            return null;
        }

        int count = 0;
        while (count < 50) {
            if (start == -1) {
                parts.add(template.substring(end));
                return new ParsedTemplate(parts, namePositions);
            }

            previousEnd = end;
            end = template.indexOf('}', start);
            if (end == -1) {
                LOGGER.warn("Template \"{}\" has \"${{\" but no matching '}}'", template);
                return null;
            }

            String name = template.substring(start + 2, end);
            int pos = templateNames.indexOf(name);
            if (pos < 0) {
                LOGGER.warn("Unknown template name \"{}\" in template \"{}\"", name, template);
                return null;
            }

            namePositions.add(pos);
            parts.add(template.substring(previousEnd, start));

            start = template.indexOf("${", end);
            end++;
            count++;
        }

        return null;
    }

    private AggMatchService() {
        // Utility class
    }
}
