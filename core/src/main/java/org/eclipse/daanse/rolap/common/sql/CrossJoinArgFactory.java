/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2004-2005 TONBELLER AG
 * Copyright (C) 2006-2017 Hitachi Vantara and others
 * All Rights Reserved.
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

package org.eclipse.daanse.rolap.common.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.daanse.mdx.model.api.expression.operation.BracesOperationAtom;
import org.eclipse.daanse.olap.api.access.AccessHierarchy;
import org.eclipse.daanse.olap.api.access.Role;
import org.eclipse.daanse.olap.api.access.RollupPolicy;
import org.eclipse.daanse.olap.api.calc.compiler.ExpressionCompiler;
import org.eclipse.daanse.olap.api.calc.tuple.TupleList;
import org.eclipse.daanse.olap.api.calc.tuple.TupleListCalc;
import org.eclipse.daanse.olap.api.element.Dimension;
import org.eclipse.daanse.olap.api.element.Hierarchy;
import org.eclipse.daanse.olap.api.element.Member;
import org.eclipse.daanse.olap.api.element.NamedSet;
import org.eclipse.daanse.olap.api.function.FunctionDefinition;
import org.eclipse.daanse.olap.api.query.component.Expression;
import org.eclipse.daanse.olap.api.query.component.HierarchyExpression;
import org.eclipse.daanse.olap.api.query.component.LevelExpression;
import org.eclipse.daanse.olap.api.DataType;
import org.eclipse.daanse.olap.api.query.component.Literal;
import org.eclipse.daanse.olap.api.query.component.MemberExpression;
import org.eclipse.daanse.olap.api.query.component.NamedSetExpression;
import org.eclipse.daanse.olap.api.query.component.NumericLiteral;
import org.eclipse.daanse.olap.api.query.component.QueryAxis;
import org.eclipse.daanse.olap.api.type.HierarchyType;
import org.eclipse.daanse.olap.api.type.Type;
import org.eclipse.daanse.olap.common.Util;
import org.eclipse.daanse.olap.fun.FunUtil;
import org.eclipse.daanse.olap.fun.sort.Sorter;
import org.eclipse.daanse.olap.function.def.parentheses.ParenthesesFunDef;
import org.eclipse.daanse.olap.function.def.set.SetFunDef;
import org.eclipse.daanse.olap.function.def.tuple.TupleFunDef;
import org.eclipse.daanse.olap.query.component.ResolvedFunCallImpl;
import org.eclipse.daanse.rolap.api.element.RolapMember;
import org.eclipse.daanse.rolap.common.evaluator.RolapEvaluator;
import org.eclipse.daanse.rolap.element.RolapCalculatedMember;
import org.eclipse.daanse.rolap.element.RolapLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates CrossJoinArgs for use in constraining SQL queries.
 *
 * @author kwalker
 * @since Dec 15, 2009
 */
public class CrossJoinArgFactory {
    protected static final Logger LOGGER =
        LoggerFactory.getLogger(CrossJoinArgFactory.class);
    private boolean restrictMemberTypes;

    public CrossJoinArgFactory(boolean restrictMemberTypes) {
        this.restrictMemberTypes = restrictMemberTypes;
    }

    public Set<CrossJoinArg> buildConstraintFromAllAxes(
        final RolapEvaluator evaluator, final boolean enableNativeFilter)
    {
        Set<CrossJoinArg> joinArgs =
            new LinkedHashSet<>();
        for (QueryAxis ax : evaluator.getQuery().getAxes()) {
            List<CrossJoinArg[]> axesArgs =
                checkCrossJoinArg(evaluator, ax.getSet(), true, enableNativeFilter);
            if (axesArgs != null) {
                for (CrossJoinArg[] axesArg : axesArgs) {
                    joinArgs.addAll(Arrays.asList(axesArg));
                }
            }
        }
        return stripConflictingArgs(joinArgs);
    }

    /**
     * Construct a set of CJ args that exclude any duplicates.  This avoids
     * construction of conflicting constraints when the same hierarchy is
     * nested on more than one axis (e.g. when appearing in a both a
     * calculation and explicitly referenced.)
     * It's not safe to include the CJ constraint in such a case because
     * we can't guess which set of members can be used in the sql constraint.
     */
    private Set<CrossJoinArg> stripConflictingArgs(Set<CrossJoinArg> joinArgs) {
        Set<Hierarchy> skip = new HashSet<>();
        Set<Hierarchy> encountered = new HashSet<>();
        // linked to keep arg order for sql
        Set<CrossJoinArg> result = new LinkedHashSet<>();
        for (CrossJoinArg arg : joinArgs) {
            if (arg.getLevel() == null) {
                continue;
            }
            Hierarchy level =  arg.getLevel().getHierarchy();
            if (encountered.contains(level) && arg.getMembers() != null) {
                skip.add(level);
            }
            encountered.add(level);
        }
        for (CrossJoinArg arg : joinArgs) {
            if (arg.getLevel() != null
                && !skip.contains(arg.getLevel().getHierarchy()))
            {
                result.add(arg);
            }
        }
        return result;
    }

    /**
     * Scans for memberChildren, levelMembers, memberDescendants, crossJoin.
     */
    public List<CrossJoinArg[]> checkCrossJoinArg(
        RolapEvaluator evaluator,
        Expression exp, final boolean enableNativeFilter)
    {
        return checkCrossJoinArg(evaluator, exp, false, enableNativeFilter);
    }

    /**
     * Checks whether an expression can be natively evaluated. The following
     * expressions can be natively evaluated:
     *
     *
     * member.Children
     * level.members
     * descendants of a member
     * member list
     * filter on a dimension
     *
     *
     * @param evaluator Evaluator
     * @param exp       Expression
     * @return List of CrossJoinArg arrays. The first array represent the
     *         CJ CrossJoinArg and the second array represent the additional
     *         constraints.
     */
    @SuppressWarnings("java:S125")
    List<CrossJoinArg[]> checkCrossJoinArg(
        RolapEvaluator evaluator,
        Expression exp,
        final boolean returnAny,
        final boolean enableNativeFilter)
    {
        if (exp instanceof NamedSetExpression namedSetExpr) {
            NamedSet namedSet = namedSetExpr.getNamedSet();
            exp = namedSet.getExp();
        }
        if (!(exp instanceof ResolvedFunCallImpl funCall)) {
            return null;
        }
        FunctionDefinition fun = funCall.getFunDef();
        Expression[] args = funCall.getArgs();

        final Role role = evaluator.getCatalogReader().getRole();
        CrossJoinArg[] cjArgs;

        cjArgs = checkMemberChildren(role, fun, args);
        if (cjArgs != null) {
            return Collections.singletonList(cjArgs);
        }
        cjArgs = checkLevelMembers(role, fun, args);
        if (cjArgs != null) {
            return Collections.singletonList(cjArgs);
        }
        cjArgs = checkDescendants(role, fun, args);
        if (cjArgs != null) {
            return Collections.singletonList(cjArgs);
        }
        final boolean exclude = false;
        cjArgs = checkEnumeration(evaluator, fun, args, exclude);
        if (cjArgs != null) {
            return Collections.singletonList(cjArgs);
        }
        cjArgs = checkRange(evaluator, fun, args);
        if (cjArgs != null) {
            return Collections.singletonList(cjArgs);
        }
        cjArgs = checkUnion(evaluator, fun, args);
        if (cjArgs != null) {
            return Collections.singletonList(cjArgs);
        }
        List<CrossJoinArg[]> exceptArgs =
            checkExcept(evaluator, fun, args, enableNativeFilter);
        if (exceptArgs != null) {
            return exceptArgs;
        }

        if (returnAny) {
            cjArgs = checkConstrainedMeasures(evaluator, fun, args);
            if (cjArgs != null) {
                return Collections.singletonList(cjArgs);
            }
        }

        List<CrossJoinArg[]> allArgs =
            checkDimensionFilter(evaluator, fun, args, enableNativeFilter);
        if (allArgs != null) {
            return allArgs;
        }
        // strip off redundant set braces, for example
        // { Gender.Gender.members }, or {{{ Gender.M }}}
        if ("{}".equalsIgnoreCase(fun.getFunctionMetaData().operationAtom().name()) && args.length == 1) {
            return checkCrossJoinArg(evaluator, args[0], returnAny, enableNativeFilter);
        }
        if ("NativizeSet".equalsIgnoreCase(fun.getFunctionMetaData().operationAtom().name()) && args.length == 1) {
            return checkCrossJoinArg(evaluator, args[0], returnAny, enableNativeFilter);
        }
        return checkCrossJoin(evaluator, fun, args, returnAny);
    }

    /**
     * Checks for a member range m1 : m2 over plain same-level
     * members. The member set comes from the same call the calc engine makes
     * ( FunUtil#memberRange  - endpoint-swap retry included), so set and
     * order are identical by construction (ranges are hierarchically ordered);
     * oversized ranges fall back to the calc engine via the maxConstraints
     * gate in  MemberListCrossJoinArg#create.
     */
    private CrossJoinArg[] checkRange(
        RolapEvaluator evaluator, FunctionDefinition fun, Expression[] args)
    {
        if (!":".equals(fun.getFunctionMetaData().operationAtom().name())
            || args.length != 2
            || !(args[0] instanceof MemberExpression firstExpr)
            || !(args[1] instanceof MemberExpression secondExpr))
        {
            return null;
        }
        final Member first = firstExpr.getMember();
        final Member second = secondExpr.getMember();
        if (first.isCalculated() || second.isCalculated()
            || first.getLevel() != second.getLevel())
        {
            return null;
        }
        List<Member> range = FunUtil.memberRange(evaluator, first, second);
        List<RolapMember> memberList = new ArrayList<>(range.size());
        for (Member member : range) {
            memberList.add((RolapMember) member);
        }
        final CrossJoinArg cjArg = MemberListCrossJoinArg.create(
            evaluator, memberList, restrictMemberTypes(), false);
        return cjArg == null ? null : new CrossJoinArg[]{cjArg};
    }

    /**
     * Checks for Union(set1, set2) - DISTINCT only; ALL keeps
     * duplicates, which a relational constraint cannot carry. Both sides must
     * reduce to one same-level include member list. The union keeps
     * first-occurrence order like the calc engine and is accepted only when
     * that order is already hierarchical - the native evaluator hierarchizes
     * its result, the calc engine does not.
     */
    private CrossJoinArg[] checkUnion(
        RolapEvaluator evaluator, FunctionDefinition fun, Expression[] args)
    {
        if (!"Union".equalsIgnoreCase(fun.getFunctionMetaData().operationAtom().name())) {
            return null;
        }
        if (args.length == 3) {
            if (!(args[2].getCategory() == DataType.SYMBOL
                    && args[2] instanceof Literal flag
                    && "DISTINCT".equalsIgnoreCase(String.valueOf(flag.getValue()))))
            {
                return null;
            }
        } else if (args.length != 2) {
            return null;
        }
        final MemberListCrossJoinArg left = singleIncludeList(evaluator, args[0]);
        final MemberListCrossJoinArg right = singleIncludeList(evaluator, args[1]);
        if (left == null || right == null
            || left.getLevel() == null || right.getLevel() == null
            || !left.getLevel().equalsOlapElement(right.getLevel()))
        {
            return null;
        }
        List<RolapMember> union = new ArrayList<>(left.getMembers());
        for (RolapMember member : right.getMembers()) {
            if (!union.contains(member)) {
                union.add(member);
            }
        }
        if (!isHierarchicallyOrdered(union)) {
            return null;
        }
        final CrossJoinArg cjArg = MemberListCrossJoinArg.create(
            evaluator, union, restrictMemberTypes(), false);
        return cjArg == null ? null : new CrossJoinArg[]{cjArg};
    }

    /**
     * Checks for Except(set1, set2), tolerating the parsed third
     * ALL symbol the same way the calc implementation does (it is ignored
     * there). Two native shapes:
     *
     *  set1 is a level-shaped arg (level.Members, member.Children,
     * Descendants) and set2 a same-level member list: emitted as the existing
     * base-arg-plus-exclude-list pair - position 1 only ever joins the WHERE
     * constraints, never the projection, and renders as NOT IN;
     *  both sides are member lists: the difference in side-0 order as one
     * include list, accepted only when that order is already hierarchical
     * (the native evaluator hierarchizes its result).
     */
    private List<CrossJoinArg[]> checkExcept(
        RolapEvaluator evaluator, FunctionDefinition fun, Expression[] args,
        boolean enableNativeFilter)
    {
        if (!"Except".equalsIgnoreCase(fun.getFunctionMetaData().operationAtom().name())
            || args.length < 2 || args.length > 3)
        {
            return null;
        }
        final MemberListCrossJoinArg subtrahend = singleIncludeList(evaluator, args[1]);
        if (subtrahend == null || subtrahend.getLevel() == null) {
            return null;
        }
        final MemberListCrossJoinArg minuendList = singleIncludeList(evaluator, args[0]);
        if (minuendList != null) {
            if (minuendList.getLevel() == null
                || !minuendList.getLevel().equalsOlapElement(subtrahend.getLevel()))
            {
                return null;
            }
            List<RolapMember> difference = new ArrayList<>(minuendList.getMembers());
            difference.removeAll(subtrahend.getMembers());
            if (!isHierarchicallyOrdered(difference)) {
                return null;
            }
            final CrossJoinArg cjArg = MemberListCrossJoinArg.create(
                evaluator, difference, restrictMemberTypes(), false);
            return cjArg == null
                ? null : Collections.singletonList(new CrossJoinArg[]{cjArg});
        }
        final List<CrossJoinArg[]> minuend =
            checkCrossJoinArg(evaluator, args[0], false, enableNativeFilter);
        if (minuend == null || minuend.size() != 1
            || minuend.get(0) == null || minuend.get(0).length != 1
            || !(minuend.get(0)[0] instanceof DescendantsCrossJoinArg base)
            || base.getLevel() == null
            || !base.getLevel().equalsOlapElement(subtrahend.getLevel()))
        {
            return null;
        }
        final CrossJoinArg excludeArg = MemberListCrossJoinArg.create(
            evaluator, new ArrayList<>(subtrahend.getMembers()),
            restrictMemberTypes(), true);
        if (excludeArg == null) {
            return null;
        }
        return Arrays.asList(minuend.get(0), new CrossJoinArg[]{excludeArg});
    }

    /**
     * Reduces an expression to one same-level include member list: an
     * enumeration {m1, m2}, also behind redundant braces. Returns
     * null for anything else.
     */
    private MemberListCrossJoinArg singleIncludeList(
        RolapEvaluator evaluator, Expression exp)
    {
        if (!(exp instanceof ResolvedFunCallImpl funCall)) {
            return null;
        }
        final CrossJoinArg[] result = checkEnumeration(
            evaluator, funCall.getFunDef(), funCall.getArgs(), false);
        if (result == null) {
            if ("{}".equalsIgnoreCase(
                    funCall.getFunDef().getFunctionMetaData().operationAtom().name())
                && funCall.getArgCount() == 1)
            {
                return singleIncludeList(evaluator, funCall.getArg(0));
            }
            return null;
        }
        if (result.length != 1
            || !(result[0] instanceof MemberListCrossJoinArg list)
            || list.isExclude())
        {
            return null;
        }
        return list;
    }

    /**
     * Whether the list already carries the order the native evaluator's
     * hierarchize pass would produce.
     */
    private static boolean isHierarchicallyOrdered(List<RolapMember> memberList) {
        List<Member> copy = new ArrayList<>(memberList);
        Sorter.hierarchizeMemberList(copy, false);
        return copy.equals(memberList);
    }

    private CrossJoinArg[] checkConstrainedMeasures(
        RolapEvaluator evaluator, FunctionDefinition fun, Expression[] args)
    {
            if (isSetOfConstrainedMeasures(fun, args)) {
            HashMap<Dimension, List<RolapMember>> memberLists =
                new LinkedHashMap<>();
            for (Expression arg : args) {
                addConstrainingMembersToMap(arg, memberLists);
            }
            return memberListCrossJoinArgArray(memberLists, args, evaluator);
        }
        return null;
    }

    private boolean isSetOfConstrainedMeasures(FunctionDefinition fun, Expression[] args) {
        return fun.getFunctionMetaData().operationAtom().name().equals("{}") && allArgsConstrainedMeasure(args);
    }

    private boolean allArgsConstrainedMeasure(Expression[] args) {
        for (Expression arg : args) {
            if (!isConstrainedMeasure(arg)) {
                return false;
            }
        }
        return true;
    }

    private boolean isConstrainedMeasure(Expression arg) {
        if (!(arg instanceof MemberExpression memberExpr
            && memberExpr.getMember().isMeasure()))
        {
            if (arg instanceof ResolvedFunCallImpl call && (call.getFunDef() instanceof SetFunDef
                || call.getFunDef() instanceof ParenthesesFunDef))
            {
                return allArgsConstrainedMeasure(call.getArgs());
            }
            return false;
        }
        Member member = ((MemberExpression) arg).getMember();
        if (member instanceof RolapCalculatedMember rolapCalculatedMember) {
            Expression calcExp =
                rolapCalculatedMember.getFormula().getExpression();
            return (calcExp instanceof ResolvedFunCallImpl resolvedFunCall
                && resolvedFunCall.getFunDef()
                instanceof TupleFunDef)
                || calcExp instanceof Literal;
        }
        return false;
    }

    private void addConstrainingMembersToMap(
        Expression arg, Map<Dimension, List<RolapMember>> memberLists)
    {
        if (arg instanceof ResolvedFunCallImpl call) {
            for (Expression callArg : call.getArgs()) {
                addConstrainingMembersToMap(callArg, memberLists);
            }
        }
        Expression[] tupleArgs = getCalculatedTupleArgs(arg);
        for (Expression tupleArg : tupleArgs) {
            Dimension dimension = tupleArg.getType().getDimension();

            if (tupleArg instanceof ResolvedFunCallImpl) {
                addConstrainingMembersToMap(tupleArg, memberLists);
            } else if (dimension != null && !dimension.isMeasures()) {
                List<RolapMember> members;
                if (memberLists.containsKey(dimension)) {
                    members = memberLists.get(dimension);
                } else {
                    members = new ArrayList<>();
                }
                members.add((RolapMember) ((MemberExpression) tupleArg).getMember());
                memberLists.put(dimension, members);
            } else if (isConstrainedMeasure(tupleArg)) {
                addConstrainingMembersToMap(tupleArg, memberLists);
            }
        }
    }

    private Expression[] getCalculatedTupleArgs(Expression arg) {
        if (arg instanceof MemberExpression memberExpr) {
            Member member = memberExpr.getMember();
            if (member instanceof RolapCalculatedMember rolapCalculatedMember) {
                Expression formulaExp =
                    rolapCalculatedMember
                        .getFormula().getExpression();
                if (formulaExp instanceof ResolvedFunCallImpl resolvedFunCall) {
                    return resolvedFunCall.getArgs();
                }
            }
        }
        return new Expression[0];
    }

    private CrossJoinArg[] memberListCrossJoinArgArray(
        Map<Dimension, List<RolapMember>> memberLists,
        Expression[] args,
        RolapEvaluator evaluator)
    {
        List<CrossJoinArg> argList = new ArrayList<>();
        for (List<RolapMember> memberList : memberLists.values()) {
            if (memberList.size() == countNonLiteralMeasures(args)) {
                // when the memberList and args list have the same length
                // it means there must have been a constraint on each measure
                // for this dimension.
                final CrossJoinArg cjArg =
                    MemberListCrossJoinArg.create(
                        evaluator,
                        removeDuplicates(memberList),
                        restrictMemberTypes(), false);
                if (cjArg != null) {
                    argList.add(cjArg);
                }
            }
        }
        if (!argList.isEmpty()) {
            return argList.toArray(CrossJoinArg[]::new);
        }
        return null;
    }

    private List<RolapMember> removeDuplicates(List<RolapMember> list)
    {
        Set<RolapMember> set = new HashSet<>();
        List<RolapMember> uniqueList = new ArrayList<>();
        for (RolapMember element : list) {
            if (set.add(element)) {
                uniqueList.add(element);
            }
        }
        return uniqueList;
    }

    private int countNonLiteralMeasures(Expression[] length) {
        int count = 0;
        for (Expression exp : length) {
            if (exp instanceof MemberExpression memberExpr) {
                Expression calcExp = memberExpr.getMember().getExpression();
                if (!(calcExp instanceof Literal)) {
                    count++;
                }
            } else if (exp instanceof ResolvedFunCallImpl resolvedFunCall) {
                count +=
                    countNonLiteralMeasures(resolvedFunCall.getArgs());
            }
        }
        return count;
    }

    /**
     * Checks for CrossJoin(&lt;set1&gt;, &lt;set2&gt;), where
     * set1 and set2 are one of
     * member.children, level.members or
     * member.descendants.
     *
     * @param evaluator Evaluator to use if inputs are to be evaluated
     * @param fun       The function, either "CrossJoin" or "NonEmptyCrossJoin"
     * @param args      Inputs to the CrossJoin
     * @param returnAny indicates we should return any valid crossjoin args
     * @return array of CrossJoinArg representing the inputs
     */
    public List<CrossJoinArg[]> checkCrossJoin(
        RolapEvaluator evaluator,
        FunctionDefinition fun,
        Expression[] args,
        final boolean returnAny)
    {
        // is this "CrossJoin([A].children, [B].children)"
        if (!"Crossjoin".equalsIgnoreCase(fun.getFunctionMetaData().operationAtom().name())
            && !"NonEmptyCrossJoin".equalsIgnoreCase(fun.getFunctionMetaData().operationAtom().name()))
        {
            return null;
        }
        if (args.length != 2) {
            return null;
        }
        // Check if the arguments can be natively evaluated.
        // If not, try evaluating this argument and turning the result into
        // MemberListCrossJoinArg.
        List<CrossJoinArg[]> allArgsOneInput;
        // An array(size 2) of arrays(size arbitary). Each outer array represent
        // native inputs fro one input.
        CrossJoinArg[][] cjArgsBothInputs =
            new CrossJoinArg[2][];
        CrossJoinArg[][] predicateArgsBothInputs =
            new CrossJoinArg[2][];

        for (int i = 0; i < 2; i++) {
            allArgsOneInput = checkCrossJoinArg(evaluator, args[i], returnAny, evaluator.getQuery().getConnection().getContext().getConfig().enableNativeFilter());

            if (allArgsOneInput == null
                || allArgsOneInput.isEmpty()
                || allArgsOneInput.getFirst() == null)
            {
                cjArgsBothInputs[i] = expandNonNative(evaluator, args[i]);
            } else {
                // Collect CJ CrossJoinArg
                cjArgsBothInputs[i] = allArgsOneInput.getFirst();
            }
            if (returnAny) {
                continue;
            }
            if (cjArgsBothInputs[i] == null) {
                return null;
            }

            // Collect Predicate CrossJoinArg if it exists.
            predicateArgsBothInputs[i] = null;
            if (allArgsOneInput != null && allArgsOneInput.size() == 2) {
                predicateArgsBothInputs[i] = allArgsOneInput.get(1);
            }
        }

        List<CrossJoinArg[]> allArgsBothInputs =
            new ArrayList<>();
        // Now combine the cjArgs from both sides
        CrossJoinArg[] combinedCJArgs =
            Util.appendArrays(
                cjArgsBothInputs[0] == null
                    ? CrossJoinArg.EMPTY_ARRAY
                    : cjArgsBothInputs[0],
                cjArgsBothInputs[1] == null
                    ? CrossJoinArg.EMPTY_ARRAY
                    : cjArgsBothInputs[1]);
        allArgsBothInputs.add(combinedCJArgs);

        CrossJoinArg[] combinedPredicateArgs =
            Util.appendArrays(
                predicateArgsBothInputs[0] == null
                    ? CrossJoinArg.EMPTY_ARRAY
                    : predicateArgsBothInputs[0],
                predicateArgsBothInputs[1] == null
                    ? CrossJoinArg.EMPTY_ARRAY
                    : predicateArgsBothInputs[1]);
        if (combinedPredicateArgs.length > 0) {
            allArgsBothInputs.add(combinedPredicateArgs);
        }

        return allArgsBothInputs;
    }

    /**
     * Checks for a set constructor, {member1, member2,
     * &#46;&#46;&#46;} that does not contain calculated members.
     *
     * @return an {@link org.eclipse.daanse.rolap.common.sql.CrossJoinArg} instance describing the enumeration,
     *         or null if fun represents something else.
     */
    private CrossJoinArg[] checkEnumeration(
        RolapEvaluator evaluator,
        FunctionDefinition fun,
        Expression[] args,
        boolean exclude)
    {
        // Return null if not the expected function name or input size.
        if (fun == null) {
            if (args.length != 1) {
                return null;
            }
        } else {
            if (!"{}".equalsIgnoreCase(fun.getFunctionMetaData().operationAtom().name())
                || !isArgSizeSupported(args.length, evaluator.getQuery().getConnection().getContext()
                    .getConfig().maxConstraints()))
            {
                return null;
            }
        }

        List<RolapMember> memberList = new ArrayList<>();
        for (Expression arg : args) {
            if (!(arg instanceof MemberExpression)) {
                return null;
            }
            final Member member = ((MemberExpression) arg).getMember();
            if (member.isCalculated()
                && !member.isParentChildLeaf())
            {
                // also returns null if any member is calculated
                return null;
            }
            memberList.add((RolapMember) member);
        }

        final CrossJoinArg cjArg =
            MemberListCrossJoinArg.create(
                evaluator, memberList, restrictMemberTypes(), exclude);
        if (cjArg == null) {
            return null;
        }
        return new CrossJoinArg[]{cjArg};
    }

    private boolean restrictMemberTypes() {
        return restrictMemberTypes;
    }

    /**
     * Checks for &lt;Member&gt;.Children.
     *
     * @return an {@link org.eclipse.daanse.rolap.common.sql.CrossJoinArg} instance describing the member.children
     *         function, or null if fun represents something else.
     */
    private CrossJoinArg[] checkMemberChildren(
        Role role,
        FunctionDefinition fun,
        Expression[] args)
    {
        if (!"Children".equalsIgnoreCase(fun.getFunctionMetaData().operationAtom().name())) {
            return null;
        }
        if (args.length != 1) {
            return null;
        }

        // Note: <Dimension>.Children is not recognized as a native expression.
        if (!(args[0] instanceof MemberExpression)) {
            return null;
        }
        RolapMember member = (RolapMember) ((MemberExpression) args[0]).getMember();
        if (member.isCalculated()) {
            return null;
        }
        RolapLevel level = member.getLevel();
        level = (RolapLevel) level.getChildLevel();
        if (level == null || !level.isSimple()) {
            // no child level
            return null;
        }
        // Children of a member in an access-controlled hierarchy cannot be
        // converted to SQL when RollupPolicy=FULL. (We could be smarter; we
        // don't currently notice when we don't look below the rolled up level
        // therefore no access-control is needed.
        final AccessHierarchy access = role.getAccess(level.getHierarchy());
        switch (access) {
        case ALL:
            break;
        case CUSTOM:
            final RollupPolicy rollupPolicy =
                role.getAccessDetails(level.getHierarchy()).getRollupPolicy();
            if (rollupPolicy == RollupPolicy.FULL) {
                return null;
            }
        break;
        default:
            return null;
        }
        return new CrossJoinArg[]{
            new DescendantsCrossJoinArg(level, member)
        };
    }

    /**
     * Checks for &lt;Level&gt;.Members.
     *
     * &lt;Hierarchy&gt;.Members counts only for an all-less single-level
     * hierarchy, where it equals the only level's Members. Any other
     * hierarchy spans the all member plus several levels, and a native arg
     * models exactly one level with a relational expression — MDX that wants
     * native evaluation there writes &lt;Level&gt;.Members.
     *
     * @return an {@link org.eclipse.daanse.rolap.common.sql.CrossJoinArg} instance describing the Level.members
     *         function, or null if fun represents something else.
     */
    private CrossJoinArg[] checkLevelMembers(
        Role role,
        FunctionDefinition fun,
        Expression[] args)
    {
        if (!"Members".equalsIgnoreCase(fun.getFunctionMetaData().operationAtom().name())) {
            return null;
        }
        if (args.length != 1) {
            return null;
        }
        RolapLevel level;
        if (args[0] instanceof LevelExpression levelExpression) {
            level = (RolapLevel) levelExpression.getLevel();
        } else if (args[0] instanceof HierarchyExpression hierarchyExpression
                && !hierarchyExpression.getHierarchy().hasAll()
                && hierarchyExpression.getHierarchy().getLevels().size() == 1) {
            // an all-less single-level hierarchy: its Members equal the only
            // level's Members
            level = (RolapLevel) hierarchyExpression.getHierarchy().getLevels().get(0);
        } else {
            return null;
        }
        if (!level.isSimple()) {
            return null;
        }
        // Members of a level in an access-controlled hierarchy cannot be
        // converted to SQL when RollupPolicy=FULL. (We could be smarter; we
        // don't currently notice when we don't look below the rolled up level
        // therefore no access-control is needed.
        final AccessHierarchy access = role.getAccess(level.getHierarchy());
        switch (access) {
        case ALL:
            break;
        case CUSTOM:
            final RollupPolicy rollupPolicy =
                role.getAccessDetails(level.getHierarchy()).getRollupPolicy();
            if (rollupPolicy == RollupPolicy.FULL) {
                return null;
            }
        break;
        default:
            return null;
        }
        return new CrossJoinArg[]{
            new DescendantsCrossJoinArg(level, null)
        };
    }


    private static boolean isArgSizeSupported(
        int argSize, int maxConstraints)
    {
        boolean argSizeNotSupported = false;

        // Note: arg size 0 is accepted as valid CJ argument
        // This is used to push down the "1 = 0" predicate
        // into the emerging CJ so that the entire CJ can
        // be natively evaluated.

        // First check that the member list will not result in a predicate
        // longer than the underlying DB could support.
        if (argSize > maxConstraints) {
            argSizeNotSupported = true;
        }

        return !argSizeNotSupported;
    }

    /**
     * Checks for Descendants(&lt;member&gt;, &lt;Level&gt;), also with a numeric depth
     * or an explicit SELF flag (the 2-arg form's default; every other flag
     * changes the result set and stays non-native).
     *
     * @return an {@link org.eclipse.daanse.rolap.common.sql.CrossJoinArg} instance describing the Descendants
     *         function, or null if fun represents something else.
     */
    private CrossJoinArg[] checkDescendants(
        Role role,
        FunctionDefinition fun,
        Expression[] args)
    {
        if (!"Descendants".equalsIgnoreCase(fun.getFunctionMetaData().operationAtom().name())) {
            return null;
        }
        if (args.length == 3) {
            if (!(args[2].getCategory() == DataType.SYMBOL
                    && args[2] instanceof Literal flag
                    && "SELF".equalsIgnoreCase(String.valueOf(flag.getValue())))) {
                return null;
            }
        } else if (args.length != 2) {
            return null;
        }
        if (!(args[0] instanceof MemberExpression)) {
            return null;
        }
        RolapMember member = (RolapMember) ((MemberExpression) args[0]).getMember();
        if (member.isCalculated()) {
            return null;
        }
        RolapLevel level = null;
        if ((args[1] instanceof LevelExpression)) {
            level = (RolapLevel) ((LevelExpression) args[1]).getLevel();
        } else if (args[1] instanceof NumericLiteral descendantsDepth) {
            List<RolapLevel> levels = (List<RolapLevel>) member.getHierarchy().getLevels();
            int currentDepth = member.getDepth();
            int newDepth = currentDepth + descendantsDepth.getIntValue();
            if (newDepth < levels.size()) {
                level = levels.get(newDepth);
            }
        } else {
            return null;
        }

        if (level == null || !level.isSimple()) {
            return null;
        }
        // Descendants of a member in an access-controlled hierarchy cannot be
        // converted to SQL. (We could be smarter; we don't currently notice
        // when the member is in a part of the hierarchy that is not
        // access-controlled.)
        final AccessHierarchy access = role.getAccess(level.getHierarchy());
        if (!AccessHierarchy.ALL.equals(access)) {
            return null;
        }
        return new CrossJoinArg[]{
            new DescendantsCrossJoinArg(level, member)
        };
    }

    /**
     * Check if a dimension filter can be natively evaluated.
     * Currently, these types of filters can be natively evaluated:
     * Filter(Set, Qualified Predicate)
     * where Qualified Predicate is either
     * CurrentMember reference IN {m1, m2},
     * CurrentMember reference Is m1,
     * negation(NOT) of qualified predicate
     * conjuction(AND) of qualified predicates
     * and where
     * currentmember reference is either a member or
     * ancester of a member from the context,
     *
     * @param evaluator  Evaluator
     * @param fun        Filter function
     * @param filterArgs inputs to the Filter function
     * @return a list of CrossJoinArg arrays. The first array is the CrossJoin
     *         dimensions. The second array, if any, contains additional
     *         constraints on the dimensions. If either the list or the first
     *         array is null, then native cross join is not feasible.
     */
    private List<CrossJoinArg[]> checkDimensionFilter(
        RolapEvaluator evaluator,
        FunctionDefinition fun,
        Expression[] filterArgs, boolean enableNativeFilter)
    {
        if (!enableNativeFilter) {
            return null;
        }

        // Return null if not the expected funciton name or input size.
        if (!"Filter".equalsIgnoreCase(fun.getFunctionMetaData().operationAtom().name())
            || filterArgs.length != 2)
        {
            return null;
        }

        // Now check filterArg[0] can be natively evaluated.
        // checkCrossJoin returns a list of CrossJoinArg arrays.
        // The first array is the CrossJoin dimensions
        // The second array, if any, contains additional constraints on the
        // dimensions. If either the list or the first array is null, then
        // native cross join is not feasible.
        List<CrossJoinArg[]> allArgs =
            checkCrossJoinArg(evaluator, filterArgs[0], enableNativeFilter);

        if (allArgs == null || allArgs.isEmpty() || allArgs.getFirst() == null) {
            return null;
        }

        final CrossJoinArg[] cjArgs = allArgs.getFirst();
        if (cjArgs == null) {
            return null;
        }

        final CrossJoinArg[] previousPredicateArgs;
        if (allArgs.size() == 2) {
            previousPredicateArgs = allArgs.get(1);
        } else {
            previousPredicateArgs = null;
        }

        // True if the Filter wants to exclude member(s)
        final boolean exclude = false;

        // Check that filterArgs[1] is a qualified predicate
        // (IS/IN with NOT/AND/OR composites where the structure allows)
        CrossJoinArg[] currentPredicateArgs;
        if (filterArgs[1] instanceof ResolvedFunCallImpl predicateCall) {
            currentPredicateArgs =
                checkFilterPredicate(evaluator, predicateCall, exclude);
        } else {
            currentPredicateArgs = null;
        }

        if (currentPredicateArgs == null) {
            return null;
        }

        // cjArgs remain the same but now there is more predicateArgs
        // Combine the previous predicate args with the current predicate args.
        LOGGER.debug("using native dimension filter");
        CrossJoinArg[] combinedPredicateArgs =
            currentPredicateArgs;

        if (previousPredicateArgs != null) {
            combinedPredicateArgs =
                Util.appendArrays(previousPredicateArgs, currentPredicateArgs);
        }

        // CJ args do not change.
        // Predicate args will grow if filter is native.
        return Arrays.asList(cjArgs, combinedPredicateArgs);
    }

    /**
     * Checks whether the filter predicate can be turned into native SQL.
     * See comment for checkDimensionFilter for the types of predicates
     * suported.
     *
     * @param evaluator     Evaluator
     * @param predicateCall Call to predicate function (ANd, NOT or parentheses)
     * @param exclude       Whether to exclude tuples that match the predicate
     * @return if filter predicate can be natively evaluated, the CrossJoinArg
     *         array representing the predicate; otherwise, null.
     */
    private CrossJoinArg[] checkFilterPredicate(
        RolapEvaluator evaluator,
        ResolvedFunCallImpl predicateCall,
        boolean exclude)
    {
        CrossJoinArg[] predicateCJArgs = null;
        if (predicateCall.getOperationAtom().name().equals("()")) {
            Expression actualPredicateCall = predicateCall.getArg(0);
            if (actualPredicateCall instanceof ResolvedFunCallImpl resolvedFunCall) {
                return checkFilterPredicate(
                    evaluator, resolvedFunCall, exclude);
            } else {
                return null;
            }
        }

        if (predicateCall.getOperationAtom().name().equals("NOT")
            && predicateCall.getArg(0) instanceof ResolvedFunCallImpl resolvedFunCall)
        {
            predicateCall = resolvedFunCall;
            // Flip the exclude flag
            exclude = !exclude;
            return checkFilterPredicate(evaluator, predicateCall, exclude);
        }

        String opName = predicateCall.getOperationAtom().name();
        if (opName.equals("AND") || opName.equals("OR")) {
            Expression arg0 = predicateCall.getArg(0);
            Expression arg1 = predicateCall.getArg(1);
            if (!(arg0 instanceof ResolvedFunCallImpl side0)
                || !(arg1 instanceof ResolvedFunCallImpl side1))
            {
                return null;
            }
            CrossJoinArg[] cjArgs0 = checkFilterPredicate(evaluator, side0, exclude);
            if (cjArgs0 == null) {
                return null;
            }
            CrossJoinArg[] cjArgs1 = checkFilterPredicate(evaluator, side1, exclude);
            if (cjArgs1 == null) {
                return null;
            }
            // Downstream every arg is a separate conjunct, so only true
            // conjunctions may concatenate: include-AND directly, and
            // exclude-OR because NOT(a OR b) = NOT a AND NOT b. The other two
            // shapes collapse into ONE same-level member-list arg:
            // include-OR as the union, exclude-AND as the intersection
            // (NOT(a AND b) = NOT IN (a ∩ b) for one current member).
            if (opName.equals("AND") != exclude) {
                return Util.appendArrays(cjArgs0, cjArgs1);
            }
            return mergeSameLevelMemberLists(evaluator, cjArgs0, cjArgs1, exclude, opName.equals("OR"));
        }

        // Now check the broken down predicate clause.
        predicateCJArgs =
            checkFilterPredicateInIs(evaluator, predicateCall, exclude);
        return predicateCJArgs;
    }

    /**
     * Collapses two one-element member-list sides of the same level into one
     * arg — the union (include-OR) or the intersection (exclude-AND). Null
     * when the sides span levels or are not plain member lists.
     */
    private CrossJoinArg[] mergeSameLevelMemberLists(
        RolapEvaluator evaluator,
        CrossJoinArg[] cjArgs0,
        CrossJoinArg[] cjArgs1,
        boolean exclude,
        boolean union)
    {
        if (cjArgs0.length != 1 || cjArgs1.length != 1
            || !(cjArgs0[0] instanceof MemberListCrossJoinArg left)
            || !(cjArgs1[0] instanceof MemberListCrossJoinArg right)
            || left.isExclude() != exclude || right.isExclude() != exclude
            || left.getLevel() == null || right.getLevel() == null
            || !left.getLevel().equalsOlapElement(right.getLevel()))
        {
            return null;
        }
        List<RolapMember> members = new ArrayList<>(left.getMembers());
        if (union) {
            for (RolapMember member : right.getMembers()) {
                if (!members.contains(member)) {
                    members.add(member);
                }
            }
        } else {
            members.retainAll(right.getMembers());
        }
        CrossJoinArg merged =
            MemberListCrossJoinArg.create(evaluator, members, restrictMemberTypes, exclude);
        return merged == null ? null : new CrossJoinArg[]{merged};
    }

    /**
     * Check whether the predicate is an IN or IS predicate and can be
     * natively evaluated.
     *
     * @return the array of CrossJoinArg containing the predicate.
     */
    private CrossJoinArg[] checkFilterPredicateInIs(
        RolapEvaluator evaluator,
        ResolvedFunCallImpl predicateCall,
        boolean exclude)
    {
        final boolean useIs;
        if (predicateCall.getOperationAtom().name().equals("IS")) {
            useIs = true;
        } else if (predicateCall.getOperationAtom().name().equals("IN")) {
            useIs = false;
        } else {
            // Neither IN nor IS
            // This predicate can not be natively evaluated.
            return null;
        }

        Expression[] predArgs = predicateCall.getArgs();
        if (predArgs.length != 2) {
            return null;
        }

        // Check that predArgs[0] is a ResolvedFuncCall while FunDef is:
        //   DimensionCurrentMemberFunDef
        //   HierarchyCurrentMemberFunDef
        //   or Ancestor of those functions.
        if (!(predArgs[0] instanceof ResolvedFunCallImpl predFirstArgCall)) {
            return null;
        }

        if (predFirstArgCall.getFunDef().getFunctionMetaData().operationAtom().name().equals("Ancestor")) {
            Expression[] ancestorArgs = predFirstArgCall.getArgs();

            if (!(ancestorArgs[0] instanceof ResolvedFunCallImpl)) {
                return null;
            }

            predFirstArgCall = (ResolvedFunCallImpl) ancestorArgs[0];
        }

        // Now check that predFirstArgCall is a CurrentMember function that
        // refers to the dimension being filtered
        FunctionDefinition predFirstArgFun = predFirstArgCall.getFunDef();
        if (!predFirstArgFun.getFunctionMetaData().operationAtom().name().equals("CurrentMember")) {
            return null;
        }

        Expression currentMemberArg = predFirstArgCall.getArg(0);
        Type currentMemberArgType = currentMemberArg.getType();

        // Input to CurremntMember should be either Dimension or Hierarchy type.
        if (!(currentMemberArgType
            instanceof org.eclipse.daanse.olap.api.type.DimensionType
            || currentMemberArgType instanceof HierarchyType))
        {
            return null;
        }

        // It is not necessary to check currentMemberArg comes from the same
        // dimension as one of the filterCJArgs, because query parser makes sure
        // that currentMember always references dimensions in context.

        // Check that predArgs[1] can be expressed as an MemberListCrossJoinArg.
        Expression predSecondArg = predArgs[1];
        Expression[] predSecondArgList;
        FunctionDefinition predSecondArgFun;
        CrossJoinArg[] predCJArgs;

        if (useIs) {
            // IS operator
            if (!(predSecondArg instanceof MemberExpression)) {
                return null;
            }

            // IS predicate only contains one member
            // Make it into a list to be uniform with IN predicate.
            predSecondArgFun = null;
            predSecondArgList = new Expression[]{predSecondArg};
        } else {
            // IN operator
            if (predSecondArg instanceof NamedSetExpression namedSetExpr) {
                NamedSet namedSet =
                    namedSetExpr.getNamedSet();
                predSecondArg = namedSet.getExp();
            }

            if (!(predSecondArg instanceof ResolvedFunCallImpl predSecondArgCall)) {
                return null;
            }

            predSecondArgFun = predSecondArgCall.getFunDef();
            predSecondArgList = predSecondArgCall.getArgs();
        }

        predCJArgs =
            checkEnumeration(
                evaluator, predSecondArgFun, predSecondArgList, exclude);
        return predCJArgs;
    }

    private CrossJoinArg[] expandNonNative(
        RolapEvaluator evaluator,
        Expression exp)
    {
        ExpressionCompiler compiler = evaluator.getQuery().createCompiler();
        CrossJoinArg[] arg0 = null;
        if (shouldExpandNonEmpty(exp,
            evaluator.getCube().getCatalog().getInternalConnection().getContext().getConfig().expandNonNative())
            && evaluator.getActiveNativeExpansions().add(exp))
        {
            TupleListCalc listCalc0 = compiler.compileList(exp);
            final TupleList tupleList = listCalc0.evaluate(evaluator);

            // Prevent the case when the second argument size is too large
            Util.checkCJResultLimit(tupleList.size());

            if (tupleList.getArity() == 1) {
                List<RolapMember> list0 =
                    Util.cast(tupleList.slice(0));
                CrossJoinArg arg =
                    MemberListCrossJoinArg.create(
                        evaluator, list0, restrictMemberTypes(), false);
                if (arg != null) {
                    arg0 = new CrossJoinArg[]{arg};
                }
            }
            evaluator.getActiveNativeExpansions().remove(exp);
        }
        return arg0;
    }

    private boolean shouldExpandNonEmpty(Expression exp, boolean expandNonNative) {
        return expandNonNative
            || isCheapSet(exp);
    }

    private boolean isCheapSet(Expression exp) {
        return isSet(exp) && allArgsCheapToExpand(exp);
    }

    private static final List<String> CHEAP_FUNS =
        List.of("LastChild", "FirstChild", "Lag");

    private boolean allArgsCheapToExpand(Expression exp) {
        while (exp instanceof NamedSetExpression namedSetExpr) {
            exp = namedSetExpr.getNamedSet().getExp();
        }
        for (Expression arg : ((ResolvedFunCallImpl) exp).getArgs()) {
            if (arg instanceof ResolvedFunCallImpl resolvedFunCall) {
                if (!CHEAP_FUNS.contains(resolvedFunCall.getOperationAtom().name())) {
                    return false;
                }
            } else if (!(arg instanceof MemberExpression)) {
                return false;
            }
        }
        return true;
    }

    private boolean isSet(Expression exp) {
        return ((exp instanceof ResolvedFunCallImpl resolvedFunCall)
            && resolvedFunCall.getOperationAtom() instanceof BracesOperationAtom)
            || (exp instanceof NamedSetExpression);
    }
}
