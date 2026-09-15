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
import java.util.function.Function;

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
import org.eclipse.daanse.olap.api.element.Member;
import org.eclipse.daanse.olap.common.ConfigConstants;
import org.eclipse.daanse.olap.evaluator.NativeEvaluatorFactory;
import org.eclipse.daanse.rolap.common.nativize.RolapNativeRegistry;
import org.eclipse.daanse.rolap.element.RolapCatalog;
import org.eclipse.daanse.rolap.mapping.instance.api.CatalogTestInstance;
import org.eclipse.daanse.rolap.testkit.assertions.ConfigOverride;
import org.eclipse.daanse.rolap.testkit.assertions.NativeVerify;
import org.eclipse.daanse.rolap.testkit.core.TestContext;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that the native TopCount/BottomCount and Filter paths
 * activate: each form produces a native evaluator, runs its tuple SQL,
 * serves the repeat from the tuple cache, matches the calc-engine result,
 * and stops when the live config disables it. NON EMPTY is not required
 * for either path.
 */
class NativeTopCountFilterActivationTest {

    private static final String CATALOG = "complex.school";

    /** (level, stored measure) → MDX. */
    private record Slot(Level level, Member measure, String cube) {
    }

    private static final class CountingListener implements NativeEvaluatorFactory.Listener {
        final AtomicInteger found = new AtomicInteger();
        final AtomicInteger sql = new AtomicInteger();
        final AtomicInteger cacheHits = new AtomicInteger();

        @Override
        public void foundEvaluator(NativeEvaluatorFactory.NativeEvent e) {
            found.incrementAndGet();
        }

        @Override
        public void foundInCache(NativeEvaluatorFactory.TupleEvent e) {
            cacheHits.incrementAndGet();
        }

        @Override
        public void executingSql(NativeEvaluatorFactory.TupleEvent e) {
            sql.incrementAndGet();
        }
    }

    @Test
    void topCountActivates() throws Exception {
        assertActivates(slot -> "SELECT {" + slot.measure().getUniqueName() + "} ON COLUMNS, TopCount("
                + slot.level().getUniqueName() + ".Members, 3, " + slot.measure().getUniqueName()
                + ") ON ROWS FROM [" + slot.cube() + "]", ConfigConstants.ENABLE_NATIVE_TOP_COUNT);
    }

    @Test
    void bottomCountStaysOnTheCalcEngine() throws Exception {
        // BottomCount is never native: ascending order picks empty-measure
        // members first (even under NON EMPTY, which filters after the
        // count), which the native fact join cannot rank
        Connection conn = connect();
        RolapNativeRegistry registry = ((RolapCatalog) conn.getCatalog()).getNativeRegistry();
        CountingListener listener = new CountingListener();
        registry.setListener(listener);
        try {
            for (Slot slot : slots(conn)) {
                execute(conn, "SELECT {" + slot.measure().getUniqueName()
                        + "} ON COLUMNS, NON EMPTY BottomCount(" + slot.level().getUniqueName()
                        + ".Members, 3, " + slot.measure().getUniqueName() + ") ON ROWS FROM ["
                        + slot.cube() + "]");
            }
            assertThat(listener.found.get()).isZero();
        } finally {
            registry.setListener(null);
        }
    }

    @Test
    void filterActivates() throws Exception {
        assertActivates(slot -> "SELECT {" + slot.measure().getUniqueName() + "} ON COLUMNS, Filter("
                + slot.level().getUniqueName() + ".Members, " + slot.measure().getUniqueName()
                + " > 0) ON ROWS FROM [" + slot.cube() + "]", ConfigConstants.ENABLE_NATIVE_FILTER);
    }

    private static void assertActivates(Function<Slot, String> mdxOf, String enableKey) throws Exception {
        Connection conn = connect();
        RolapNativeRegistry registry = ((RolapCatalog) conn.getCatalog()).getNativeRegistry();
        CountingListener listener = new CountingListener();
        registry.setListener(listener);
        String nativeMdx = null;
        try {
            List<String> candidates = new ArrayList<>();
            for (Slot slot : slots(conn)) {
                candidates.add(mdxOf.apply(slot));
            }
            assertThat(candidates).isNotEmpty();
            for (String mdx : candidates) {
                int before = listener.found.get();
                execute(conn, mdx);
                if (listener.found.get() > before) {
                    nativeMdx = mdx;
                    break;
                }
            }
            assertThat(nativeMdx).as("no candidate went native on %s; candidates: %s", CATALOG, candidates)
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
        NativeVerify.assertSameNativeAndNot(conn, nativeMdx, "native activation");

        // live config: disabling the flag stops the native path without a catalog rebuild
        CountingListener disabled = new CountingListener();
        registry.setListener(disabled);
        try (ConfigOverride override = ConfigOverride.of(conn.getContext()).set(enableKey, false)) {
            execute(conn, nativeMdx);
            assertThat(disabled.found.get()).as("disabled flag must suppress the native evaluator").isZero();
        } finally {
            registry.setListener(null);
        }
    }

    /** Per cube: topmost non-all level of every hierarchy plus the first stored measure. */
    private static List<Slot> slots(Connection conn) {
        List<Slot> result = new ArrayList<>();
        for (Cube cube : conn.getCatalog().getCubes()) {
            Member measure = cube.getMeasures().stream().filter(m -> !m.isCalculated()).findFirst().orElse(null);
            if (measure == null) {
                continue;
            }
            for (Dimension dimension : cube.getDimensions()) {
                if (dimension.isMeasures()) {
                    continue;
                }
                for (Hierarchy hierarchy : dimension.getHierarchies()) {
                    hierarchy.getLevels().stream().filter(l -> !l.isAll()).findFirst()
                            .ifPresent(level -> result.add(new Slot(level, measure, cube.getName())));
                    break;
                }
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
