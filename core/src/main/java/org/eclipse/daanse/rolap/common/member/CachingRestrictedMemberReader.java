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


package org.eclipse.daanse.rolap.common.member;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.eclipse.daanse.olap.util.Pair;

import org.eclipse.daanse.olap.api.access.AccessMember;
import org.eclipse.daanse.olap.api.access.Role;
import org.eclipse.daanse.olap.api.element.Member;
import org.eclipse.daanse.rolap.api.element.RolapMember;
import org.eclipse.daanse.rolap.common.cache.BoundedCache;
import org.eclipse.daanse.rolap.common.sql.MemberChildrenConstraint;
import org.eclipse.daanse.rolap.element.RolapHierarchy.LimitedRollupMember;

/**
 * A {@link CachingRestrictedMemberReader} is a subclass of
 * {@link RestrictedMemberReader} which caches the access rights
 * per children's list. We place them in this throw-away object
 * to speed up partial rollup calculations.
 *
 * The speed improvement is noticeable when dealing with very
 * big dimensions with a lot of branches (like a parent-child
 * hierarchy) because the 'partial' rollup policy forces us to
 * navigate the tree and find the lowest level to rollup to and
 * then figure out all of the children on which to constraint
 * the SQL query.
 */
public class CachingRestrictedMemberReader extends RestrictedMemberReader {

    public CachingRestrictedMemberReader(
        final MemberReader memberReader,
        final Role role)
    {
        // We want to extend a RestrictedMemberReader with access details
        // that we cache.
        super(memberReader, role);
    }

    // Size-capped: the values strongly reach their keys (children reference
    // the parent), so reference-based eviction can never fire here — the cap
    // bounds memory instead. Keyed by (member, constraint cache key): a list
    // loaded under a narrowing constraint must never serve a broader request.
    private static final int MAX_CACHED_MEMBERS = 1000;

    final BoundedCache<Pair<RolapMember, Object>, AccessAwareMemberList>
        memberToChildren = BoundedCache.ofEntries(MAX_CACHED_MEMBERS);

    /**
     * Flush fence: the load registry of the caching reader beneath bumps
     * its generation on every member flush; a generation change here drops
     * this role-filtered cache, which no flush path reaches directly.
     */
    private final MemberLoadRegistry flushFence = MemberLoadRegistry.of(this);
    private volatile long fenceGeneration;


    void dropCacheIfFlushed() {
        if (flushFence != null) {
            final long generation = flushFence.generation();
            if (generation != fenceGeneration) {
                memberToChildren.clear();
                fenceGeneration = generation;
            }
        }
    }

    @Override
    public Map<? extends Member, AccessMember> getMemberChildren(
        RolapMember member,
        List<RolapMember> children,
        MemberChildrenConstraint constraint)
    {
        // Strip off the rollup wrapper.
        if (member instanceof LimitedRollupMember) {
            member = ((LimitedRollupMember)member).member;
        }
        dropCacheIfFlushed();
        final Object constraintCacheKey =
            constraint == null ? null : constraint.getCacheKey();
        if (constraint != null && constraintCacheKey == null) {
            // not cacheable by contract (see MemberListCache): storing under
            // Pair(member, null) would collide with the "no constraint" slot
            return super.getMemberChildren(member, children, constraint);
        }
        final Pair<RolapMember, Object> key = Pair.of(member, constraintCacheKey);
        AccessAwareMemberList memberList = memberToChildren.get(key);
        if (memberList != null) {
            children.addAll(memberList.children);
            return memberList.accessMap;
        }

        // Miss: load lock-free (SQL), then publish. A concurrent loader of
        // the same key just overwrites with an equal value. The generation
        // is captured BEFORE the load and re-checked at publish time (the
        // roots-memo pattern): reader A snapshots g0, a flush bumps to g1,
        // reader B clears and stamps g1 - without the recheck, A then
        // published its pre-flush list, which lived unbounded.
        final long loadGeneration = flushFence == null ? 0 : flushFence.generation();
        Map<? extends Member, AccessMember> membersWithAccessDetails =
            super.getMemberChildren(
                member,
                children,
                constraint);

        if (flushFence == null || flushFence.generation() == loadGeneration) {
            // explicit element cast: the access map is typed over Member,
            // but this reader only ever sees RolapMembers
            List<RolapMember> childrenCopy =
                new ArrayList<>(membersWithAccessDetails.size());
            for (Member accessMember : membersWithAccessDetails.keySet()) {
                childrenCopy.add((RolapMember) accessMember);
            }
            memberToChildren.put(
                key,
                new AccessAwareMemberList(membersWithAccessDetails, childrenCopy));
        }
        return membersWithAccessDetails;
    }

    static class AccessAwareMemberList {
        private final Map<? extends Member, AccessMember> accessMap;
        private final Collection<RolapMember> children;
        public AccessAwareMemberList(
            Map<? extends Member, AccessMember> accessMap,
            Collection<RolapMember> children)
        {
            this.accessMap = accessMap;
            this.children = children;
        }
    }
}
