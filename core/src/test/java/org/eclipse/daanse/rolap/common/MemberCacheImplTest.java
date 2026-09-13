/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2006-2017 Hitachi Vantara and others
 * All Rights Reserved.
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

package org.eclipse.daanse.rolap.common;

import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.daanse.rolap.common.member.MemberLoadRegistry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.daanse.rolap.api.element.RolapMember;
import org.eclipse.daanse.rolap.common.constraint.DefaultMemberChildrenConstraint;
import org.eclipse.daanse.rolap.common.member.MemberCacheImpl;
import org.eclipse.daanse.rolap.common.member.MemberKeyR;
import org.eclipse.daanse.rolap.common.sql.MemberChildrenConstraint;
import org.eclipse.daanse.rolap.element.RolapLevel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


class MemberCacheImplTest {

    @Mock
    private RolapMember parentMember;

    @Mock
    private TestPublicChildByNameConstraint childByNameConstraint;

    private MemberChildrenConstraint defMemChildrenConstraint =
        DefaultMemberChildrenConstraint.instance();

    private List<RolapMember> children = new ArrayList<>();

    private MemberCacheImpl memberCache = new MemberCacheImpl(null);

    @BeforeEach void beforeEach() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void roundtripChildrenUsingChildByNameConstraint() {
        List<String> childNames = fillChildren(children, 3);
        when(childByNameConstraint.getChildNames()).thenReturn(childNames);

        memberCache.putChildren(parentMember, childByNameConstraint, children);

        List<RolapMember> retrievedChildren =
            memberCache.getChildrenFromCache(
                parentMember, childByNameConstraint);

        assertThat(retrievedChildren).isEqualTo(children);
    }

    @Test
    void cachedByDefaultConstraint() {
        List<String> childNames = fillChildren(children, 5);
        when(childByNameConstraint.getChildNames()).thenReturn(childNames);

        // cached under default constraint, but subsequent
        // retrieval with childByName should work since all children present.
        memberCache.putChildren(
            parentMember,
            defMemChildrenConstraint, children);

        List<RolapMember> retrievedChildren =
            memberCache.getChildrenFromCache(
                parentMember,
                childByNameConstraint);

        assertThat(retrievedChildren).isEqualTo(children);
    }

    @Test
    void onlyRequestedChildrenRetrieved() {
        // tests retrieval of a subset of children from
        // the cache with keyed with DefaultMemberChildrenConstraint
        List<String> childNames = fillChildren(children, 5);

        int FROM = 2;
        int TO = 5;
        // childByName constraint defined with member names in sublist
        when(childByNameConstraint.getChildNames()).thenReturn(
            childNames.subList(FROM, TO));

        // cached under default constraint, but subsequent
        // retrieval with childByName should work since all children present.
        memberCache.putChildren(
            parentMember,
            defMemChildrenConstraint, children);

        List<RolapMember> retrievedChildren =
            memberCache.getChildrenFromCache(
                parentMember,
                childByNameConstraint);

        assertThat(retrievedChildren).as("Expected children were not retrieved from cache.").isEqualTo(children.subList(FROM, TO));
    }

    @Test
    void missingChildrenNotRetrievedDefaultConst() {
        runMissingChildrenNotRetrievedTest(defMemChildrenConstraint);
    }

    @Test
    void missingChildrenNotRetrievedChildByName() {
        runMissingChildrenNotRetrievedTest(childByNameConstraint);
    }

    public void runMissingChildrenNotRetrievedTest(
        MemberChildrenConstraint constraint)
    {
        fillChildren(children, 5);
        when(childByNameConstraint.getChildNames()).thenReturn(
            Arrays.asList(new String[]{ "Other Name", "Other Name2" }));

        memberCache.putChildren(
            parentMember, constraint, children);

        List<RolapMember> retrievedChildren =
            memberCache.getChildrenFromCache(
                parentMember, childByNameConstraint);

        assertThat(retrievedChildren).as("Not expecting to retrieve anything from cache").isNull();
    }


    @Test
    void removeChildMemberPresentInNamedChildrenMap() {
        List<String> childNames = fillChildren(children, 3);
        when(childByNameConstraint.getChildNames()).thenReturn(
            childNames.subList(1, 3));
        List<MemberKeyR> childKeys = new ArrayList<>();

        for (RolapMember member : children) {
            when(member.getParentMember()).thenReturn(parentMember);
            MemberKeyR key = mockMemberKey();
            childKeys.add(key);
            memberCache.putMember(key, member);
        }
        MemberKeyR parentKey = new MemberKeyR(null, "parentValue");
        memberCache.putMember(parentKey, parentMember);
        memberCache.putChildren(parentMember, childByNameConstraint, children);

        memberCache.removeMember(childKeys.getFirst());

        List<RolapMember> members =
            memberCache.getChildrenFromCache(
                parentMember, childByNameConstraint);

        assertThat(members).as("Retrieved children should not include the removed member").isEqualTo(children.subList(1, 3));
    }

    private static int keyCounter = 0;

    private MemberKeyR mockMemberKey() {
        RolapMember parent = mock(RolapMember.class);
        RolapLevel mockLevel = mock(RolapLevel.class);
        when(mockLevel.isParentChild()).thenReturn(true);
        when(parent.getLevel()).thenReturn(mockLevel);
        return new MemberKeyR(parent, "key-" + (keyCounter++));
    }

    private List<String> fillChildren(List<RolapMember> children, int count) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            RolapMember member = mock(RolapMember.class);
            String name = "Member-" + i;
            names.add(name);
            when(member.getName()).thenReturn(name);
            when(member.getUniqueName()).thenReturn("[P].[" + name + "]");
            // there's a bug in mockito which causes mock objects
            // .compareTo to always return 1
            // https://code.google.com/p/mockito/issues/detail?id=467
            // here's a workaround.
            when(member.compareTo(any(RolapMember.class))).thenAnswer(
                new Answer<Object>() {
                    @Override
					public Object answer(InvocationOnMock invocation)
                        throws Throwable
                    {
                        return  ((RolapMember)invocation.getMock()).getName()
                            .compareTo(
                                ((RolapMember) invocation.getArguments()[0])
                                    .getName());
                    }
                }
            );
            children.add(member);
        }
        return names;
    }

    /**
     * A closed fence (flush raced an in-flight load) must not re-seed a
     * member - but a member cached BEFORE the fence closed stays the
     * identity. Red while the fenced path returned the caller's fresh
     * copy even though a canonical member sat in the map.
     */
    @Test
    void fencedPutMemberStillCanonicalizesToTheCachedMember() {
        RolapMember cached = mock(RolapMember.class);
        MemberKeyR key = new MemberKeyR(null, "canonical");
        assertThat(memberCache.putMember(key, cached)).isSameAs(cached);

        MemberLoadRegistry registry =
            new MemberLoadRegistry();
        memberCache.setLoadRegistry(registry);
        registry.enterLoad();
        try {
            registry.bumpGeneration();

            RolapMember copy = mock(RolapMember.class);
            assertThat(memberCache.putMember(key, copy))
                .as("the pre-fence member stays the identity")
                .isSameAs(cached);

            MemberKeyR unknown = new MemberKeyR(null, "unknown");
            RolapMember fresh = mock(RolapMember.class);
            assertThat(memberCache.putMember(unknown, fresh))
                .as("an unknown member is handed back unseeded")
                .isSameAs(fresh);
            assertThat(memberCache.getMember(unknown))
                .as("the fence still blocks the write")
                .isNull();
        } finally {
            registry.exitLoad();
        }
    }

    /**
     * Two siblings with DISTINCT keys but the same name (a nameColumn
     * level) share a uniqueName, so the named-children set orders by key
     * as well: both members stay, and findNamedChildrenInCache reports a
     * hit only when it holds all of them.
     */
    @Test
    void sameNamedSiblingsWithDistinctKeysBothSurvive() {
        RolapMember smith1 = mock(RolapMember.class);
        RolapMember smith2 = mock(RolapMember.class);
        int i = 0;
        for (RolapMember m : List.of(smith1, smith2)) {
            i++;
            when(m.getName()).thenReturn("John Smith");
            when(m.getUniqueName()).thenReturn("[Emp].[John Smith]");
            when(m.getKey()).thenReturn("employee-" + i);
            when(m.getParentMember()).thenReturn(parentMember);
            when(m.compareTo(any(RolapMember.class))).thenAnswer(inv ->
                String.valueOf(((RolapMember) inv.getMock()).getKey())
                    .compareTo(String.valueOf(
                        ((RolapMember) inv.getArguments()[0]).getKey())));
        }
        when(childByNameConstraint.getChildNames())
            .thenReturn(List.of("John Smith"));

        memberCache.putChildren(parentMember, childByNameConstraint,
            List.of(smith1, smith2));

        // both siblings survive the set, so a one-name lookup cannot be
        // satisfied from it: the correct answer is a MISS (null, reload) -
        // the collapsed set served a single-member HIT instead
        List<RolapMember> found = memberCache.getChildrenFromCache(
            parentMember, childByNameConstraint);
        assertThat(found)
            .as("two same-named siblings must produce a miss, not a one-member hit")
            .isNull();
    }

    /**
     * Divergent load paths can type the same key column differently
     * (Integer via the fact table, String via an aggregate) - key
     * RENDERINGS then collide ("1" == String.valueOf(1)) although the
     * keys differ. The class-name tiebreak keeps both members in the
     * set; the lookup answers with a MISS like in the same-type case.
     */
    @Test
    void mixedKeyTypeSiblingsWithCollidingRenderingsBothSurvive() {
        RolapMember intKeyed = mock(RolapMember.class);
        RolapMember stringKeyed = mock(RolapMember.class);
        when(intKeyed.getKey()).thenReturn(1);
        when(stringKeyed.getKey()).thenReturn("1");
        for (RolapMember m : List.of(intKeyed, stringKeyed)) {
            when(m.getName()).thenReturn("One");
            when(m.getUniqueName()).thenReturn("[N].[One]");
            when(m.getParentMember()).thenReturn(parentMember);
        }
        when(childByNameConstraint.getChildNames()).thenReturn(List.of("One"));

        memberCache.putChildren(parentMember, childByNameConstraint,
            List.of(intKeyed, stringKeyed));

        assertThat(memberCache.getChildrenFromCache(
                parentMember, childByNameConstraint))
            .as("colliding renderings across key types must not collapse "
                + "to a one-member hit")
            .isNull();
    }

    /**
     * The named-children set is prunable regardless of comparator drift:
     * a member's natural ordering can differ between the (bound) query
     * thread that built the set and the (unbound) flush thread that prunes
     * it, so removal searches by equality under a stable comparator
     * removal.
     */
    @Test
    void removeMemberSurvivesComparatorDriftInNamedChildren() {
        // members whose compareTo flips between two orderings, emulating
        // the thread-scoped caseSensitive config
        AtomicBoolean caseSensitive =
            new AtomicBoolean(true);
        List<RolapMember> drifting = new ArrayList<>();
        String[] names = { "B", "a", "c" };
        for (String name : names) {
            RolapMember m = mock(RolapMember.class);
            when(m.getName()).thenReturn(name);
            when(m.getUniqueName()).thenReturn("[P].[" + name + "]");
            when(m.getParentMember()).thenReturn(parentMember);
            when(m.compareTo(any(RolapMember.class))).thenAnswer(inv -> {
                String self = ((RolapMember) inv.getMock()).getName();
                String other = ((RolapMember) inv.getArguments()[0]).getName();
                return caseSensitive.get()
                    ? self.compareTo(other)
                    : self.compareToIgnoreCase(other);
            });
            drifting.add(m);
        }
        when(childByNameConstraint.getChildNames())
            .thenReturn(Arrays.asList("B", "a", "c"));
        List<MemberKeyR> keys = new ArrayList<>();
        for (RolapMember m : drifting) {
            MemberKeyR key = mockMemberKey();
            keys.add(key);
            memberCache.putMember(key, m);
        }
        // build the set under the "case sensitive" ordering
        memberCache.putChildren(parentMember, childByNameConstraint, drifting);

        // ... and prune under the drifted ordering
        caseSensitive.set(false);
        memberCache.removeMember(keys.getFirst());

        // the key mapping is gone in every legal outcome - this half of the
        // assertion cannot be voided by a nuked children entry
        assertThat(memberCache.getMember(keys.getFirst()))
            .as("the removed member must leave the key cache")
            .isNull();
        // the children entry either shrank (member pruned) or was nuked on a
        // lost CAS race (null = miss, reload) - both are correct; only a
        // surviving stale member is the regression
        List<RolapMember> remaining =
            memberCache.getChildrenFromCache(parentMember, childByNameConstraint);
        if (remaining != null) {
            assertThat(remaining)
                .as("the removed member must not survive the prune")
                .doesNotContain(drifting.getFirst());
        }
    }

    /**
     * removeMember REPLACES cached lists, never mutates them: readers
     * iterate a cached list without any lock, and the rewrite re-weighs
     * the entry.
     */
    @Test
    void removeMemberReplacesCachedListsInsteadOfMutating() {
        fillChildren(children, 3);
        List<MemberKeyR> childKeys = new ArrayList<>();
        for (RolapMember member : children) {
            when(member.getParentMember()).thenReturn(parentMember);
            MemberKeyR key = mockMemberKey();
            childKeys.add(key);
            memberCache.putMember(key, member);
        }
        MemberChildrenConstraint constraint =
            DefaultMemberChildrenConstraint.instance();
        memberCache.putChildren(parentMember, constraint, children);
        List<RolapMember> before =
            memberCache.getChildrenFromCache(parentMember, constraint);
        assertThat(before).hasSize(3);

        memberCache.removeMember(childKeys.getFirst());

        assertThat(before).as("a reader's list reference must stay intact").hasSize(3);
        assertThat(memberCache.getChildrenFromCache(parentMember, constraint)).hasSize(2);
    }

    @Test
    void duplicateNamedSiblingsDoNotMaskAMissingName() {
        RolapMember firstA = org.mockito.Mockito.mock(RolapMember.class);
        org.mockito.Mockito.when(firstA.getName()).thenReturn("A");
        org.mockito.Mockito.when(firstA.getUniqueName()).thenReturn("[P].[A].&k1");
        RolapMember secondA = org.mockito.Mockito.mock(RolapMember.class);
        org.mockito.Mockito.when(secondA.getName()).thenReturn("A");
        org.mockito.Mockito.when(secondA.getUniqueName()).thenReturn("[P].[A].&k2");
        memberCache.putChildren(parentMember,
            new org.eclipse.daanse.rolap.common.constraint.ChildByNameConstraint(
                new org.eclipse.daanse.olap.query.component.IdImpl.NameSegmentImpl("A")),
            List.of(firstA, secondA));

        // two cached hits share the name "A": the COUNT matches a two-name
        // request, but "B" is not covered - this must be a miss, not a hit
        // that silently drops "B"
        org.assertj.core.api.Assertions.assertThat(
            memberCache.getChildrenFromCache(parentMember,
                new org.eclipse.daanse.rolap.common.constraint.ChildByNameConstraint(List.of(
                    new org.eclipse.daanse.olap.query.component.IdImpl.NameSegmentImpl("A"),
                    new org.eclipse.daanse.olap.query.component.IdImpl.NameSegmentImpl("B")))))
            .isNull();
        // and the settled round-5 semantics stay: a one-name lookup over
        // two same-named siblings is a MISS too (count mismatch)
        org.assertj.core.api.Assertions.assertThat(
            memberCache.getChildrenFromCache(parentMember,
                new org.eclipse.daanse.rolap.common.constraint.ChildByNameConstraint(
                    new org.eclipse.daanse.olap.query.component.IdImpl.NameSegmentImpl("A"))))
            .isNull();
    }
}
