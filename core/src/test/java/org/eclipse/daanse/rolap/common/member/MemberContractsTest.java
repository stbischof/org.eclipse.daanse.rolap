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
import static org.mockito.Mockito.when;

import java.util.List;

import org.eclipse.daanse.rolap.api.element.RolapMember;
import org.eclipse.daanse.rolap.element.RolapLevel;
import org.junit.jupiter.api.Test;

/**
 * The small contracts other code relies on blindly: the no-op cache's
 * canonicalization answer, the key's level derivation, the registry's
 * key release after completion.
 */
class MemberContractsTest {

    /**
     * CacheControlImpl.removeFromCache and putMember callers rely on
     * these without checking the implementation: putMember returns the
     * OFFERED instance (canonicalization contract), removeMember answers
     * null instead of throwing.
     */
    @Test
    void theNoOpCacheHonoursTheCanonicalizationContract() {
        // the no-op MemberCache contract lives on the no-cache reader
        // itself since the K5 fix (it IS its own cache; removeMember
        // additionally bumps the fence registry but still returns null)
        MemberReader source = mock(MemberReader.class);
        org.mockito.Mockito.when(source.setCache(org.mockito.Mockito.any())).thenReturn(true);
        NoCacheMemberReader cache = new NoCacheMemberReader(source);
        RolapMember offered = mock(RolapMember.class);
        Object key = cache.makeKey(null, "k");

        assertThat(cache.putMember(key, offered)).isSameAs(offered);
        assertThat(cache.removeMember(key)).isNull();
        assertThat(cache.getMember(key)).isNull();
    }

    /**
     * MemberKeyR.getLevel drives removeMember's children-map pruning:
     * root members have no level, parent-child levels keep the PARENT's
     * level (children live on the same level), plain members use the
     * child level.
     */
    @Test
    void memberKeyLevelDerivation() {
        assertThat(new MemberKeyR(null, "root").getLevel()).isNull();

        RolapLevel parentChild = mock(RolapLevel.class);
        when(parentChild.isParentChild()).thenReturn(true);
        RolapMember pcParent = mock(RolapMember.class);
        when(pcParent.getLevel()).thenReturn(parentChild);
        assertThat(new MemberKeyR(pcParent, "child").getLevel())
            .as("parent-child: the child lives on the SAME level")
            .isSameAs(parentChild);

        RolapLevel plain = mock(RolapLevel.class);
        RolapLevel childLevel = mock(RolapLevel.class);
        when(plain.isParentChild()).thenReturn(false);
        when(plain.getChildLevel()).thenReturn(childLevel);
        RolapMember parent = mock(RolapMember.class);
        when(parent.getLevel()).thenReturn(plain);
        assertThat(new MemberKeyR(parent, "child").getLevel())
            .isSameAs(childLevel);
    }

    /** After complete() the key is free: the NEXT claimant loads again. */
    @Test
    void aCompletedKeyIsClaimableAgain() {
        MemberLoadRegistry registry = new MemberLoadRegistry();
        MemberLoadRegistry.Claim first = registry.claim("key");
        assertThat(first.loader()).isTrue();
        registry.complete(first, List.of());

        MemberLoadRegistry.Claim second = registry.claim("key");
        assertThat(second.loader())
            .as("completion releases the key for a fresh load")
            .isTrue();
        registry.complete(second, List.of());
    }

    /**
     * complete() with an already-spent claim must neither pass silently
     * nor release the SUCCESSOR's slot: the slot future rejects the
     * second put loudly, and the two-argument map remove leaves the
     * successor's in-flight entry untouched.
     */
    @Test
    void aStaleCompletionNeitherPassesNorReleasesTheSuccessor() {
        MemberLoadRegistry registry = new MemberLoadRegistry();
        MemberLoadRegistry.Claim stale = registry.claim("key");
        registry.complete(stale, List.of());

        MemberLoadRegistry.Claim successor = registry.claim("key");
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> registry.complete(stale, List.of()));
        // the successor's claim still owns the key - a waiter joining now
        // attaches to the successor, not to a released slot
        MemberLoadRegistry.Claim waiter = registry.claim("key");
        assertThat(waiter.loader())
            .as("the successor's in-flight slot survives a stale complete")
            .isFalse();
        registry.complete(successor, List.of());
    }
}
