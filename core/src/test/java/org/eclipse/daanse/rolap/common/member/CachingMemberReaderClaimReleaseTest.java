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
package org.eclipse.daanse.rolap.common.member;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.daanse.rolap.common.sql.MemberChildrenConstraint;
import org.eclipse.daanse.rolap.element.RolapHierarchy;
import org.eclipse.daanse.rolap.api.element.RolapMember;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The claim loop of loadMemberChildren keeps the counted-prefix
 * discipline: a throw while probing the k-th parent's cache releases the
 * owner claims already taken for parents 0..k-1 - a leaked claim would
 * strand every later claimer of that key for the full await timeout.
 */
class CachingMemberReaderClaimReleaseTest {

    @Test
    void aThrowFromTheCacheProbeReleasesTheAlreadyTakenClaims() {
        MemberReader source = mock(MemberReader.class);
        when(source.getHierarchy()).thenReturn(
            mock(RolapHierarchy.class, Mockito.RETURNS_DEEP_STUBS));
        when(source.setCache(Mockito.any())).thenReturn(true);
        MemberCacheImpl throwing = mock(MemberCacheImpl.class);
        CachingMemberReader reader = new CachingMemberReader(source) {
            @Override
            protected MemberCacheImpl childrenCache() {
                return throwing;
            }
        };
        RolapMember first = mock(RolapMember.class);
        RolapMember second = mock(RolapMember.class);
        MemberChildrenConstraint constraint = mock(MemberChildrenConstraint.class);
        when(constraint.getCacheKey()).thenReturn("k");
        // pre-probe: both miss; claim-loop re-probe: first misses (claim
        // taken, parent queued for SQL), second explodes
        when(throwing.getChildrenFromCache(same(first), Mockito.any()))
            .thenReturn(null);
        when(throwing.getChildrenFromCache(same(second), Mockito.any()))
            .thenReturn(null)
            .thenThrow(new IllegalStateException("cache probe blew up"));

        assertThatThrownBy(() -> reader.getMemberChildren(
                List.of(first, second), new ArrayList<>(), constraint))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("cache probe blew up");

        // the first parent's claim was taken before the throw and must be
        // released again: a fresh claim is a LOADER, not a stranded waiter
        MemberLoadRegistry.Claim reclaimed = reader.loadRegistry().claim(
            new MemberLoadRegistry.ChildrenKey(first, "k"));
        assertThat(reclaimed.loader()).isTrue();
    }
}
