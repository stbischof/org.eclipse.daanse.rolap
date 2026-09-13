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
package org.eclipse.daanse.rolap.testkit.nativize;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.daanse.cwm.testkit.api.DataSupplier;
import org.eclipse.daanse.cwm.testkit.api.DatabaseSupplier;
import org.eclipse.daanse.cwm.testkit.data.DataLayer;
import org.eclipse.daanse.cwm.testkit.database.DatabaseLayer;
import org.eclipse.daanse.jdbc.datasource.testkit.api.ActiveDatabase;
import org.eclipse.daanse.jdbc.datasource.testkit.api.DatabaseProvider;
import org.eclipse.daanse.olap.api.Context;
import org.eclipse.daanse.olap.api.connection.Connection;
import org.eclipse.daanse.olap.api.element.Cube;
import org.eclipse.daanse.olap.api.element.Dimension;
import org.eclipse.daanse.olap.api.element.Hierarchy;
import org.eclipse.daanse.olap.api.element.Level;
import org.eclipse.daanse.olap.common.ConfigConstants;
import org.eclipse.daanse.rolap.common.nativize.RolapNative;
import org.eclipse.daanse.rolap.common.nativize.RolapNativeRegistry;
import org.eclipse.daanse.rolap.element.RolapCatalog;
import org.eclipse.daanse.rolap.mapping.instance.api.CatalogTestInstance;
import org.eclipse.daanse.rolap.testkit.assertions.ConfigOverride;
import org.eclipse.daanse.rolap.testkit.assertions.NativeVerify;
import org.eclipse.daanse.rolap.testkit.core.TestContext;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that the native SQL path activates: a NON EMPTY crossjoin
 * of two level member sets produces a native evaluator, runs its tuple SQL,
 * serves the repeat from the native set cache, matches the calc-engine
 * result, and stops immediately when the live config disables it.
 */
class NativeCrossJoinActivationTest {

    private static final String CATALOG = "complex.school";

    private static final class CountingListener implements RolapNative.Listener {
        final AtomicInteger found = new AtomicInteger();
        final AtomicInteger sql = new AtomicInteger();
        final AtomicInteger cacheHits = new AtomicInteger();

        @Override
        public void foundEvaluator(RolapNative.NativeEvent e) {
            found.incrementAndGet();
        }

        @Override
        public void foundInCache(RolapNative.TupleEvent e) {
            cacheHits.incrementAndGet();
        }

        @Override
        public void executingSql(RolapNative.TupleEvent e) {
            sql.incrementAndGet();
        }
    }

    @Test
    void nonEmptyLevelCrossJoinRunsNatively() throws Exception {
        Connection conn = connect();
        RolapNativeRegistry registry = ((RolapCatalog) conn.getCatalog()).getNativeRegistry();
        CountingListener listener = new CountingListener();
        registry.setListener(listener);
        String nativeMdx = null;
        try {
            List<String> candidates = buildCandidates(conn);
            assertThat(candidates).isNotEmpty();
            for (String mdx : candidates) {
                int before = listener.found.get();
                execute(conn, mdx);
                if (listener.found.get() > before) {
                    nativeMdx = mdx;
                    break;
                }
            }
            assertThat(nativeMdx)
                    .as("no NON EMPTY level crossjoin went native on %s; candidates: %s", CATALOG, candidates)
                    .isNotNull();
            assertThat(listener.sql.get()).as("native evaluator must run its tuple SQL").isPositive();

            // the identical query is answered from the native set tuple cache
            int hitsBefore = listener.cacheHits.get();
            execute(conn, nativeMdx);
            assertThat(listener.cacheHits.get()).as("repeat run must hit the native set cache")
                    .isGreaterThan(hitsBefore);
        } finally {
            registry.setListener(null);
        }

        // the native rendering equals the calc-engine one
        NativeVerify.assertSameNativeAndNot(conn, nativeMdx, "native crossjoin activation");

        // live config: disabling the flags stops the native path without a catalog rebuild
        CountingListener disabled = new CountingListener();
        registry.setListener(disabled);
        try (ConfigOverride override = ConfigOverride.of(conn.getContext())
                .set(ConfigConstants.ENABLE_NATIVE_CROSS_JOIN, false)
                .set(ConfigConstants.ENABLE_NATIVE_NON_EMPTY, false)) {
            execute(conn, nativeMdx);
            assertThat(disabled.found.get()).as("disabled flags must suppress the native evaluator").isZero();
        } finally {
            registry.setListener(null);
        }
    }

    /**
     * One query per hierarchy pair and cube: NON EMPTY crossjoin of the two
     * topmost non-all levels — the recognizable native form (level members,
     * no Head, non-empty axis).
     */
    private static List<String> buildCandidates(Connection conn) {
        List<String> result = new ArrayList<>();
        for (Cube cube : conn.getCatalog().getCubes()) {
            List<Level> levels = new ArrayList<>();
            for (Dimension dimension : cube.getDimensions()) {
                if (dimension.isMeasures()) {
                    continue;
                }
                for (Hierarchy hierarchy : dimension.getHierarchies()) {
                    hierarchy.getLevels().stream().filter(l -> !l.isAll()).findFirst().ifPresent(levels::add);
                    break;
                }
            }
            for (int i = 0; i + 1 < levels.size() && i < 3; i++) {
                result.add("SELECT [Measures].Members ON COLUMNS, NON EMPTY Crossjoin("
                        + levels.get(i).getUniqueName() + ".Members, " + levels.get(i + 1).getUniqueName()
                        + ".Members) ON ROWS FROM [" + cube.getName() + "]");
            }
        }
        return result;
    }

    private static void execute(Connection conn, String mdx) {
        conn.execute(conn.parseQuery(mdx));
    }

    private static Connection connect() throws Exception {
        CatalogTestInstance instance = find(CATALOG);
        ActiveDatabase db = DatabaseProvider.selected().activate(CATALOG);
        DatabaseSupplier dbSup = instance.databaseSupplier();
        DatabaseLayer.apply(db.dataSource(), db.dialect(), dbSup.schema());
        DataSupplier dataSup = instance.dataSupplier();
        if (dataSup != null) {
            DataLayer.apply(db.dataSource(), db.dialect(), dbSup.schema(), dataSup);
        }
        TestContext ctx = new TestContext(db.connectionPool(), db.dialect(), instance.mappingSupplier());
        return ((Context<?>) ctx).getConnectionWithDefaultRole();
    }

    private static CatalogTestInstance find(String name) {
        for (CatalogTestInstance instance : ServiceLoader.load(CatalogTestInstance.class)) {
            if (name.equals(instance.name())) {
                return instance;
            }
        }
        throw new IllegalStateException("CatalogTestInstance not found: " + name);
    }
}
