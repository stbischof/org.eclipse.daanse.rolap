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
package org.eclipse.daanse.rolap.common.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Constructor;
import java.util.List;

import org.eclipse.daanse.rolap.api.element.RolapMember;
import org.eclipse.daanse.rolap.element.RolapLevel;
import org.junit.jupiter.api.Test;

/**
 * Value semantics of the native-cache key component: two args over equal
 * member lists must be equal (cache hits across queries), a strict prefix
 * must NOT be (a wrong hit would return a wrong result).
 */
class MemberListCrossJoinArgEqualityTest {

    private static final RolapLevel LEVEL = mock(RolapLevel.class);
    private static final RolapMember A = mock(RolapMember.class);
    private static final RolapMember B = mock(RolapMember.class);
    private static final RolapMember C = mock(RolapMember.class);

    private static MemberListCrossJoinArg arg(List<RolapMember> members, boolean restrict, boolean exclude)
            throws Exception {
        Constructor<MemberListCrossJoinArg> ctor = MemberListCrossJoinArg.class.getDeclaredConstructor(
                RolapLevel.class, List.class,
                boolean.class, boolean.class, boolean.class, boolean.class, boolean.class);
        ctor.setAccessible(true);
        return ctor.newInstance(LEVEL, members, restrict, false, true, false, exclude);
    }

    @Test
    void equalListsFromDistinctInstancesAreEqual() throws Exception {
        MemberListCrossJoinArg first = arg(List.of(A, B), false, false);
        MemberListCrossJoinArg second = arg(List.of(A, B), false, false);
        assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
    }

    @Test
    void prefixListIsNotEqual() throws Exception {
        MemberListCrossJoinArg shorter = arg(List.of(A, B), false, false);
        MemberListCrossJoinArg longer = arg(List.of(A, B, C), false, false);
        assertThat(shorter).isNotEqualTo(longer);
        assertThat(longer).isNotEqualTo(shorter);
    }

    @Test
    void flagsAndLevelDiscriminate() throws Exception {
        MemberListCrossJoinArg base = arg(List.of(A), false, false);
        assertThat(base).isNotEqualTo(arg(List.of(A), true, false));
        assertThat(base).isNotEqualTo(arg(List.of(A), false, true));

        Constructor<MemberListCrossJoinArg> ctor = MemberListCrossJoinArg.class.getDeclaredConstructor(
                RolapLevel.class, List.class,
                boolean.class, boolean.class, boolean.class, boolean.class, boolean.class);
        ctor.setAccessible(true);
        MemberListCrossJoinArg otherLevel =
                ctor.newInstance(mock(RolapLevel.class), List.of(A), false, false, true, false, false);
        assertThat(base).isNotEqualTo(otherLevel);
    }

    @Test
    void emptyListsCompareByLevelAndFlags() throws Exception {
        MemberListCrossJoinArg first = arg(List.of(), false, false);
        MemberListCrossJoinArg second = arg(List.of(), false, false);
        assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
        assertThat(first).isNotEqualTo(arg(List.of(), false, true));
    }
}
