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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.eclipse.daanse.olap.api.access.Role;
import org.eclipse.daanse.olap.api.element.Catalog;
import org.eclipse.daanse.olap.api.element.Dimension;
import org.eclipse.daanse.olap.api.element.Hierarchy;
import org.eclipse.daanse.olap.api.element.Level;
import org.eclipse.daanse.olap.util.Pair;
import org.eclipse.daanse.rolap.api.element.RolapMember;
import org.eclipse.daanse.rolap.element.RolapHierarchy;
import org.junit.jupiter.api.Test;

/**
 * The role-filtered children cache has no direct flush path; it fences on
 * the load-registry generation of the caching reader beneath, which every
 * member flush bumps.
 */
class CachingRestrictedMemberReaderFenceTest {

    /**
     * Publish-time recheck (the roots-memo pattern): a flush that lands
     * WHILE the SQL load runs keeps the pre-flush list out of the cache -
     * the publisher compares the generation it sampled before its read
     * and cleared, and A then published its stale list - which no later
     * flush check could tell from a fresh one.
     */
    @Test
    void aLoadOvertakenByAFlushIsNotPublished() {
        Catalog catalog = mock(Catalog.class);
        Dimension dimension = mock(Dimension.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        List<? extends Level> levels = new ArrayList<>();
        levels.add(null);
        doReturn(catalog).when(dimension).getCatalog();
        doReturn(dimension).when(hierarchy).getDimension();
        doReturn(levels).when(hierarchy).getLevels();
        doReturn(true).when(hierarchy).isRagged();

        CachingMemberReader caching = mock(CachingMemberReader.class);
        doReturn(hierarchy).when(caching).getHierarchy();
        MemberLoadRegistry registry = new MemberLoadRegistry();
        doReturn(registry).when(caching).loadRegistry();
        // the flush lands while the delegate load is running
        org.mockito.Mockito.doAnswer(inv -> {
            registry.bumpGeneration();
            return Map.of();
        }).when(caching).getMemberChildren(
            org.mockito.Mockito.any(RolapMember.class),
            org.mockito.Mockito.anyList(),
            org.mockito.Mockito.any());

        Role role = mock(Role.class);
        doReturn(null).when(role).getAccessDetails(org.mockito.Mockito.any(Hierarchy.class));

        CachingRestrictedMemberReader reader =
            new CachingRestrictedMemberReader(caching, role);
        RolapMember parent = mock(RolapMember.class);
        reader.getMemberChildren(parent, new ArrayList<>(), null);

        assertThat(reader.memberToChildren.get(Pair.of(parent, (Object) null)))
            .as("the pre-flush list must not be published")
            .isNull();
    }

    /**
     * A children list loaded under a NARROWING constraint must never
     * serve a broader request: the cache key carries the constraint's
     * cache key, and the null-constraint (unconstrained) entry is a
     * separate slot.
     */
    @Test
    void constraintScopedEntriesDoNotServeBroaderRequests() {
        Catalog catalog = mock(Catalog.class);
        Dimension dimension = mock(Dimension.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        List<? extends Level> levels = new ArrayList<>();
        levels.add(null);
        doReturn(catalog).when(dimension).getCatalog();
        doReturn(dimension).when(hierarchy).getDimension();
        doReturn(levels).when(hierarchy).getLevels();
        doReturn(true).when(hierarchy).isRagged();

        CachingMemberReader caching = mock(CachingMemberReader.class);
        doReturn(hierarchy).when(caching).getHierarchy();
        doReturn(new MemberLoadRegistry()).when(caching).loadRegistry();

        Role role = mock(Role.class);
        doReturn(null).when(role).getAccessDetails(org.mockito.Mockito.any(Hierarchy.class));
        CachingRestrictedMemberReader reader =
            new CachingRestrictedMemberReader(caching, role);

        RolapMember parent = mock(RolapMember.class);
        reader.memberToChildren.put(Pair.of(parent, (Object) "narrow"),
            new CachingRestrictedMemberReader.AccessAwareMemberList(
                Map.of(), List.of()));

        assertThat(reader.memberToChildren.get(Pair.of(parent, (Object) "broad")))
            .as("a different constraint key is a different slot")
            .isNull();
        assertThat(reader.memberToChildren.get(Pair.of(parent, (Object) null)))
            .as("the unconstrained request has its own slot")
            .isNull();
        assertThat(reader.memberToChildren.get(Pair.of(parent, (Object) "narrow")))
            .isNotNull();
    }

    @Test
    void aGenerationBumpDropsTheRoleFilteredCache() {
        Catalog catalog = mock(Catalog.class);
        Dimension dimension = mock(Dimension.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        List<? extends Level> levels = new ArrayList<>();
        levels.add(null);
        doReturn(catalog).when(dimension).getCatalog();
        doReturn(dimension).when(hierarchy).getDimension();
        doReturn(levels).when(hierarchy).getLevels();
        doReturn(true).when(hierarchy).isRagged();

        CachingMemberReader caching = mock(CachingMemberReader.class);
        doReturn(hierarchy).when(caching).getHierarchy();
        MemberLoadRegistry registry = new MemberLoadRegistry();
        doReturn(registry).when(caching).loadRegistry();

        Role role = mock(Role.class);
        doReturn(null).when(role).getAccessDetails(org.mockito.Mockito.any(Hierarchy.class));

        CachingRestrictedMemberReader reader =
            new CachingRestrictedMemberReader(caching, role);

        Pair<RolapMember, Object> key =
            Pair.of(mock(RolapMember.class), "constraint");
        reader.memberToChildren.put(key,
            new CachingRestrictedMemberReader.AccessAwareMemberList(
                Map.of(), List.of()));

        // same generation: the entry survives the fence check
        reader.dropCacheIfFlushed();
        assertThat(reader.memberToChildren.get(key)).isNotNull();

        // a flush bumps the generation: the next check drops the cache
        registry.bumpGeneration();
        reader.dropCacheIfFlushed();
        assertThat(reader.memberToChildren.get(key)).isNull();

        // and the fence re-arms at the new generation
        reader.memberToChildren.put(key,
            new CachingRestrictedMemberReader.AccessAwareMemberList(
                Map.of(), List.of()));
        reader.dropCacheIfFlushed();
        assertThat(reader.memberToChildren.get(key)).isNotNull();
    }

    @Test
    void aConstraintWithoutACacheKeyIsNotStoredUnderTheNoConstraintSlot() {
        Catalog catalog = mock(Catalog.class);
        Dimension dimension = mock(Dimension.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        List<? extends Level> levels = new ArrayList<>();
        levels.add(null);
        doReturn(catalog).when(dimension).getCatalog();
        doReturn(dimension).when(hierarchy).getDimension();
        doReturn(levels).when(hierarchy).getLevels();
        doReturn(true).when(hierarchy).isRagged();
        CachingMemberReader caching = mock(CachingMemberReader.class);
        doReturn(hierarchy).when(caching).getHierarchy();
        doReturn(new MemberLoadRegistry()).when(caching).loadRegistry();
        doReturn(java.util.Map.of()).when(caching).getMemberChildren(
            org.mockito.Mockito.any(RolapMember.class),
            org.mockito.Mockito.anyList(),
            org.mockito.Mockito.any());
        Role role = mock(Role.class);
        doReturn(null).when(role).getAccessDetails(org.mockito.Mockito.any(Hierarchy.class));
        CachingRestrictedMemberReader reader =
            new CachingRestrictedMemberReader(caching, role);
        RolapMember parent = mock(RolapMember.class);
        org.eclipse.daanse.rolap.common.sql.MemberChildrenConstraint uncacheable =
            mock(org.eclipse.daanse.rolap.common.sql.MemberChildrenConstraint.class);
        doReturn(null).when(uncacheable).getCacheKey();

        reader.getMemberChildren(parent, new ArrayList<>(), uncacheable);

        // a null cache key means "not cacheable" (see MemberListCache):
        // storing it anyway would land in Pair(member, null) - the exact
        // slot the constraint-less lookup reads
        assertThat(reader.memberToChildren.get(
            org.eclipse.daanse.olap.util.Pair.of(parent, (Object) null)))
            .isNull();
    }

    /**
     * K5, closed narrowly: with members=off the delegate is a
     * NoCacheMemberReader, which used to end the registry walk - the
     * restricted reader's 1000-entry children cache then had NO flush
     * fence, and member edits (which REQUIRE members=off) never
     * invalidated it. The no-cache reader now carries a registry that
     * removeMember bumps; the stacked reader fences on it.
     */
    @Test
    void membersOffStillFencesTheRestrictedReadersCache() {
        Catalog catalog = mock(Catalog.class);
        Dimension dimension = mock(Dimension.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        List<? extends Level> levels = new ArrayList<>();
        levels.add(null);
        doReturn(catalog).when(dimension).getCatalog();
        doReturn(dimension).when(hierarchy).getDimension();
        doReturn(levels).when(hierarchy).getLevels();
        doReturn(true).when(hierarchy).isRagged();
        MemberReader source = mock(MemberReader.class);
        doReturn(true).when(source).setCache(org.mockito.Mockito.any());
        doReturn(hierarchy).when(source).getHierarchy();
        doReturn(java.util.Map.of()).when(source).getMemberChildren(
            org.mockito.Mockito.any(RolapMember.class),
            org.mockito.Mockito.anyList(),
            org.mockito.Mockito.any());
        NoCacheMemberReader noCache = new NoCacheMemberReader(source);
        Role role = mock(Role.class);
        doReturn(null).when(role).getAccessDetails(org.mockito.Mockito.any(Hierarchy.class));

        CachingRestrictedMemberReader reader =
            new CachingRestrictedMemberReader(noCache, role);
        RolapMember parent = mock(RolapMember.class);
        org.eclipse.daanse.rolap.common.sql.MemberChildrenConstraint cacheable =
            mock(org.eclipse.daanse.rolap.common.sql.MemberChildrenConstraint.class);
        doReturn("k").when(cacheable).getCacheKey();

        reader.getMemberChildren(parent, new ArrayList<>(), cacheable);
        reader.getMemberChildren(parent, new ArrayList<>(), cacheable);
        assertThat(delegateReads(source)).as("second read is a warm hit").isEqualTo(1);

        // flushMember reaches removeMember via the MemberCache interface;
        // this test drives that interface channel on the base reader (the
        // cube variant additionally wires getRolapCubeMemberCache() == this
        // in its constructor - a plain assignment, not asserted here)
        ((org.eclipse.daanse.rolap.common.member.MemberCache) noCache).removeMember("any");

        reader.getMemberChildren(parent, new ArrayList<>(), cacheable);
        assertThat(delegateReads(source)).as("post-edit read reloads").isEqualTo(2);
    }

    private static long delegateReads(MemberReader source) {
        return org.mockito.Mockito.mockingDetails(source).getInvocations().stream()
            .filter(invocation -> "getMemberChildren".equals(invocation.getMethod().getName()))
            .count();
    }
}
