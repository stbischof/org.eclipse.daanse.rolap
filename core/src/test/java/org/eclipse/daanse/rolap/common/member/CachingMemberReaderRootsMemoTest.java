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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.eclipse.daanse.rolap.api.element.RolapMember;
import org.eclipse.daanse.rolap.element.RolapHierarchy;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The root-members memo is generation-stamped: a flush (or a member
 * removal, which only bumps the generation) invalidates it, and a memo
 * read across an intervening flush is never installed. The plain
 * null-check memo kept serving a deleted root member for the life of
 * the reader.
 */
class CachingMemberReaderRootsMemoTest {

    private static CachingMemberReader reader(MemberReader source) {
        when(source.getHierarchy()).thenReturn(
            mock(RolapHierarchy.class, Mockito.RETURNS_DEEP_STUBS));
        when(source.setCache(Mockito.any())).thenReturn(true);
        return new CachingMemberReader(source);
    }

    /** A generation bump (what removeMember does) invalidates the roots memo. */
    @Test
    void generationBumpInvalidatesTheMemo() {
        MemberReader source = mock(MemberReader.class);
        RolapMember root = mock(RolapMember.class);
        when(source.getRootMembers()).thenReturn(List.of(root));
        CachingMemberReader reader = reader(source);

        assertThat(reader.getRootMembers()).containsExactly(root);
        assertThat(reader.getRootMembers()).containsExactly(root);
        verify(source, times(1)).getRootMembers();

        reader.loadRegistry().bumpGeneration(); // what removeMember does

        reader.getRootMembers();
        verify(source, times(2)).getRootMembers();
    }

    /** A load racing a flush does not install its pre-flush roots. */
    @Test
    void memoReadAcrossAFlushIsNotInstalled() {
        MemberReader source = mock(MemberReader.class);
        RolapMember stale = mock(RolapMember.class);
        CachingMemberReader[] readerHolder = new CachingMemberReader[1];
        when(source.getRootMembers()).thenAnswer(inv -> {
            // a flush lands while the roots are being read
            readerHolder[0].loadRegistry().bumpGeneration();
            return List.of(stale);
        });
        CachingMemberReader reader = reader(source);
        readerHolder[0] = reader;

        reader.getRootMembers();
        // the racy result was returned but NOT memoized: the next call
        // re-reads instead of serving the pre-flush list
        reader.getRootMembers();
        verify(source, times(2)).getRootMembers();
    }
}
