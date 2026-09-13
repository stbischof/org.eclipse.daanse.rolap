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
package org.eclipse.daanse.rolap.common.constraint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.eclipse.daanse.olap.api.query.NameSegment;
import org.eclipse.daanse.olap.query.component.IdImpl;
import org.eclipse.daanse.rolap.api.element.RolapMember;
import org.eclipse.daanse.rolap.common.member.MemberLoadRegistry;
import org.junit.jupiter.api.Test;

/**
 * The by-name cache key is value based: independently built constraints for
 * the same names share one cache entry and one deduplicated load, and the
 * single-name form equals the one-element multi-name form.
 */
class ChildByNameConstraintKeyTest {

    private static NameSegment segment(String name) {
        return new IdImpl.NameSegmentImpl(name);
    }

    @Test
    void sameNamesShareTheKey() {
        ChildByNameConstraint first =
            new ChildByNameConstraint(List.of(segment("A"), segment("B")));
        ChildByNameConstraint second =
            new ChildByNameConstraint(List.of(segment("A"), segment("B")));

        assertThat(first.getCacheKey()).isEqualTo(second.getCacheKey())
            .hasSameHashCodeAs(second.getCacheKey());
        assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
    }

    @Test
    void differentNamesDiffer() {
        ChildByNameConstraint ab =
            new ChildByNameConstraint(List.of(segment("A"), segment("B")));
        ChildByNameConstraint ac =
            new ChildByNameConstraint(List.of(segment("A"), segment("C")));

        assertThat(ab.getCacheKey()).isNotEqualTo(ac.getCacheKey());
        assertThat(ab).isNotEqualTo(ac);
    }

    @Test
    void singleNameEqualsOneElementList() {
        ChildByNameConstraint single = new ChildByNameConstraint(segment("A"));
        ChildByNameConstraint list = new ChildByNameConstraint(List.of(segment("A")));

        assertThat(single.getCacheKey()).isEqualTo(list.getCacheKey());
        assertThat(single).isEqualTo(list);
    }

    @Test
    void loadRegistryDeduplicatesAcrossInstances() {
        RolapMember parent = mock(RolapMember.class);
        MemberLoadRegistry.ChildrenKey first = new MemberLoadRegistry.ChildrenKey(parent,
            new ChildByNameConstraint(List.of(segment("A"), segment("B"))).getCacheKey());
        MemberLoadRegistry.ChildrenKey second = new MemberLoadRegistry.ChildrenKey(parent,
            new ChildByNameConstraint(List.of(segment("A"), segment("B"))).getCacheKey());

        assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
    }
}
