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

package org.eclipse.daanse.rolap.common.nativize;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.eclipse.daanse.olap.api.ContextConfig;
import org.eclipse.daanse.olap.api.evaluator.Evaluator;
import org.eclipse.daanse.olap.api.evaluator.NativeEvaluator;
import org.eclipse.daanse.olap.api.function.FunctionDefinition;
import org.eclipse.daanse.olap.api.query.component.Expression;
import org.eclipse.daanse.olap.evaluator.NativeEvaluatorFactory;
import org.eclipse.daanse.rolap.common.evaluator.RolapEvaluator;

/**
 * Composite of {@link NativeEvaluatorFactory}s. Uses chain of responsibility
 * to select the appropriate {@link NativeEvaluatorFactory} evaluator. The evaluator
 * map is frozen at construction, so every lookup is lock-free.
 */
public class RolapNativeRegistry extends NativeEvaluatorFactory {

    private final Map<String, NativeEvaluatorFactory> nativeEvaluatorMap;

    /**
     * The enable flags are read live from the config on every createEvaluator
     * call; nativeTupleCacheMaxTuples is a cache construction parameter and
     * stays fixed for the registry's lifetime.
     */
    public RolapNativeRegistry(ContextConfig config, int nativeTupleCacheMaxTuples) {
        super.setEnabled(true);
        Map<String, NativeEvaluatorFactory> map = new LinkedHashMap<>();
        // one instance under both names: identical constraints share the
        // same tuple cache
        RolapNativeCrossJoin nativeCrossJoin =
            new RolapNativeCrossJoin(config::enableNativeCrossJoin, nativeTupleCacheMaxTuples);
        map.put(upper("NonEmptyCrossJoin"), nativeCrossJoin);
        map.put(upper("CrossJoin"), nativeCrossJoin);
        // one instance under both names: the impl derives ASC/DESC from the
        // function name
        RolapNativeTopCount nativeTopCount =
            new RolapNativeTopCount(config::enableNativeTopCount, nativeTupleCacheMaxTuples);
        map.put(upper("TopCount"), nativeTopCount);
        map.put(upper("BottomCount"), nativeTopCount);
        map.put(upper("Filter"),
            new RolapNativeFilter(config::enableNativeFilter, nativeTupleCacheMaxTuples));
        this.nativeEvaluatorMap = Map.copyOf(map);
    }

    private static String upper(String name) {
        return name.toUpperCase(Locale.ROOT);
    }

    /**
     * Returns the matching NativeEvaluator or null if fun can not
     * be executed in SQL for the given context and arguments.
     */
    @Override
	public NativeEvaluator createEvaluator(
        Evaluator evaluator, FunctionDefinition fun, Expression[] args, final boolean enableNativeFilter)
    {
        if (!isEnabled()) {
            return null;
        }

        NativeEvaluatorFactory rn = nativeEvaluatorMap.get(upper(fun.getFunctionMetaData().operationAtom().name()));
        if (rn == null) {
            return null;
        }

        NativeEvaluator ne = rn.createEvaluator(evaluator, fun, args, enableNativeFilter);

        if (ne != null && listener != null) {
            listener.foundEvaluator(new NativeEvent(this));
        }
        return ne;
    }

    /**
     * Tuple-cache stats per registered evaluator; an instance registered
     * under several names is reported once.
     */
    public Map<String, com.github.benmanes.caffeine.cache.stats.CacheStats> nativeCacheStats() {
        Map<String, com.github.benmanes.caffeine.cache.stats.CacheStats> stats = new LinkedHashMap<>();
        Set<NativeEvaluatorFactory> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Map.Entry<String, NativeEvaluatorFactory> entry : nativeEvaluatorMap.entrySet()) {
            if (entry.getValue() instanceof RolapNativeSet set && seen.add(set)) {
                stats.put(entry.getKey(), set.cacheStats());
            }
        }
        return stats;
    }

    /** for testing */
    @Override
    public
	void setListener(Listener listener) {
        super.setListener(listener);
        for (NativeEvaluatorFactory rn : nativeEvaluatorMap.values()) {
            rn.setListener(listener);
        }
    }


    /** Drops every evaluator's tuple cache. */
    public void flushNativeSetCaches() {
        forEachDistinctSet(RolapNativeSet::flushCache);
    }

    /** One visit per instance - registration under several names is normal. */
    private void forEachDistinctSet(java.util.function.Consumer<RolapNativeSet> action) {
        Set<NativeEvaluatorFactory> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (NativeEvaluatorFactory rolapNative : nativeEvaluatorMap.values()) {
            if (rolapNative instanceof RolapNativeSet set && seen.add(set)) {
                action.accept(set);
            }
        }
    }
}
