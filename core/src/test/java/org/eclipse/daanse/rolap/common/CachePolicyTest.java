/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *   Stefan Bischof (bipolis.org) - initial
 */
package org.eclipse.daanse.rolap.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.eclipse.daanse.olap.api.ContextConfig;
import org.eclipse.daanse.olap.api.element.MetaData;
import org.eclipse.daanse.olap.element.OlapMetaDataBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CachePolicyTest {

    private ContextConfig config;

    @BeforeEach
    void setUp() {
        config = mock(ContextConfig.class);
        when(config.disableCaching()).thenReturn(false);
        when(config.enableRolapCubeMemberCache()).thenReturn(true);
    }

    private static MetaData meta(Map<String, Object> tags) {
        MetaData metaData = mock(MetaData.class);
        tags.forEach((k, v) -> when(metaData.get(k)).thenReturn(v));
        return metaData;
    }

    private static MetaData empty() {
        return OlapMetaDataBase.empty();
    }

    @Test
    void withoutTagsTheGlobalDefaultsApply() {
        CachePolicy policy = CachePolicy.resolve(empty(), empty(), config);
        assertThat(policy.members()).isTrue();
        assertThat(policy.cells()).isTrue();
        assertThat(policy.nativeSets()).isTrue();
        assertThat(policy.levelPreCacheThreshold()).isEmpty();
    }

    @Test
    void cubeTagSwitchesOneLevelOff() {
        CachePolicy policy = CachePolicy.resolve(empty(),
                meta(Map.of(CachePolicy.TAG_MEMBERS, "off")), config);
        assertThat(policy.members()).isFalse();
        assertThat(policy.cells()).isTrue();
    }

    @Test
    void hierarchyMembersTagOverridesTheCubePolicy() {
        CachePolicy cubeOn = CachePolicy.resolve(empty(), empty(), config);
        assertThat(CachePolicy.membersFor(meta(Map.of(CachePolicy.TAG_MEMBERS, "off")), cubeOn)).isFalse();
        assertThat(CachePolicy.membersFor(empty(), cubeOn)).isTrue();

        CachePolicy cubeOff = CachePolicy.resolve(empty(),
                meta(Map.of(CachePolicy.TAG_MEMBERS, "off")), config);
        assertThat(CachePolicy.membersFor(meta(Map.of(CachePolicy.TAG_MEMBERS, "on")), cubeOff)).isTrue();
        // unparsable falls back to the cube policy
        assertThat(CachePolicy.membersFor(meta(Map.of(CachePolicy.TAG_MEMBERS, "maybe")), cubeOff)).isFalse();
    }

    @Test
    void hierarchyThresholdTagWinsOverTheCubeTag() {
        CachePolicy cube = CachePolicy.resolve(empty(),
                meta(Map.of(CachePolicy.TAG_LEVEL_PRECACHE_THRESHOLD, "300")), config);
        assertThat(CachePolicy.levelPreCacheThresholdFor(
                meta(Map.of(CachePolicy.TAG_LEVEL_PRECACHE_THRESHOLD, "7")), cube)).hasValue(7);
        assertThat(CachePolicy.levelPreCacheThresholdFor(empty(), cube)).hasValue(300);
        // unparsable hierarchy tag falls back to the cube tag
        assertThat(CachePolicy.levelPreCacheThresholdFor(
                meta(Map.of(CachePolicy.TAG_LEVEL_PRECACHE_THRESHOLD, "many")), cube)).hasValue(300);
        assertThat(CachePolicy.levelPreCacheThresholdFor(empty(),
                CachePolicy.resolve(empty(), empty(), config))).isEmpty();
    }

    @Test
    void killSwitchBeatsEveryTag() {
        when(config.disableCaching()).thenReturn(true);
        CachePolicy policy = CachePolicy.resolve(empty(),
                meta(Map.of(CachePolicy.TAG_MEMBERS, "on", CachePolicy.TAG_CELLS, "on")), config);
        assertThat(policy.members()).isFalse();
        assertThat(policy.cells()).isFalse();
        assertThat(policy.nativeSets()).isFalse();
    }

    @Test
    void catalogModeOffIsTheDefaultTheCubeTagOverrides() {
        MetaData catalog = meta(Map.of(CachePolicy.TAG_MODE, "off"));
        CachePolicy defaulted = CachePolicy.resolve(catalog, empty(), config);
        assertThat(defaulted.members()).isFalse();
        assertThat(defaulted.cells()).isFalse();

        CachePolicy overridden = CachePolicy.resolve(catalog,
                meta(Map.of(CachePolicy.TAG_CELLS, "on")), config);
        assertThat(overridden.cells()).isTrue();
        assertThat(overridden.members()).isFalse();
    }

    @Test
    void invalidValuesFallBackToTheDefault() {
        CachePolicy policy = CachePolicy.resolve(empty(),
                meta(Map.of(CachePolicy.TAG_MEMBERS, "maybe", CachePolicy.TAG_LEVEL_PRECACHE_THRESHOLD, "many")),
                config);
        assertThat(policy.members()).isTrue();
        assertThat(policy.levelPreCacheThreshold()).isEmpty();
    }

    @Test
    void thresholdTagIsParsed() {
        CachePolicy policy = CachePolicy.resolve(empty(),
                meta(Map.of(CachePolicy.TAG_LEVEL_PRECACHE_THRESHOLD, "300")), config);
        assertThat(policy.levelPreCacheThreshold()).hasValue(300);
    }
}
