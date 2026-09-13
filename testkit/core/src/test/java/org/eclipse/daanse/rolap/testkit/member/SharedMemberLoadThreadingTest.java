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
 *   SmartCity Jena, Stefan Bischof - initial
 */
package org.eclipse.daanse.rolap.testkit.member;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import javax.sql.DataSource;

import org.eclipse.daanse.cwm.testkit.database.DatabaseLayer;
import org.eclipse.daanse.jdbc.datasource.testkit.api.ActiveDatabase;
import org.eclipse.daanse.jdbc.datasource.testkit.api.DatabaseProvider;
import org.eclipse.daanse.olap.api.Context;
import org.eclipse.daanse.olap.api.connection.Connection;
import org.eclipse.daanse.olap.api.element.Member;
import org.eclipse.daanse.olap.api.result.Result;
import org.eclipse.daanse.rolap.common.RolapUtil;
import org.eclipse.daanse.rolap.testkit.core.TestContext;
import org.junit.jupiter.api.Test;

/**
 * Per-key member loading under concurrency: several threads that need the
 * same cold member list share ONE SQL load (the first claimant runs it, the
 * rest await the same future) and end up with identical member instances.
 * The baseline pass on a separate cold database establishes how many
 * dimension-only statements one load costs; the parallel pass must not
 * exceed it.
 */
class SharedMemberLoadThreadingTest {

    private static final int THREADS = 6;

    @Test
    void concurrentColdLoadOfTheSameLevelRunsTheSqlOnce() throws Exception {
        // baseline: one cold run, count the dimension-only statements
        Connection baseline = connect("MemberLoadBaseline");
        List<String> baselineSql = new CopyOnWriteArrayList<>();
        RolapUtil.setHook(baseline.getContext(), baselineSql::add);
        try {
            query(baseline);
        } finally {
            RolapUtil.setHook(baseline.getContext(), null);
        }
        long baselineDimLoads = dimOnly(baselineSql);
        assertThat(baselineDimLoads).isPositive();

        // parallel: THREADS cold queries at once must not load more often
        Connection connection = connect("MemberLoadParallel");
        List<String> capturedSql = new CopyOnWriteArrayList<>();
        CountDownLatch start = new CountDownLatch(1);
        RolapUtil.setHook(connection.getContext(), capturedSql::add);
        List<CompletableFuture<List<Member>>> futures;
        try {
            futures = java.util.stream.IntStream.range(0, THREADS)
                    .mapToObj(i -> CompletableFuture.supplyAsync(() -> {
                        try {
                            start.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(e);
                        }
                        return query(connection);
                    }))
                    .toList();
            start.countDown();
            for (CompletableFuture<List<Member>> future : futures) {
                future.join();
            }
        } finally {
            RolapUtil.setHook(connection.getContext(), null);
        }

        assertThat(dimOnly(capturedSql))
                .as("dimension loads under concurrency, baseline %s, captured %s", baselineDimLoads, capturedSql)
                .isLessThanOrEqualTo(baselineDimLoads);

        // every thread sees the same canonical member instances
        List<Member> first = futures.get(0).join();
        for (CompletableFuture<List<Member>> future : futures) {
            List<Member> members = future.join();
            assertThat(members).hasSameSizeAs(first);
            for (int i = 0; i < members.size(); i++) {
                assertThat(members.get(i)).isSameAs(first.get(i));
            }
        }
    }

    /** Statements touching only the dimension table: member-list loads and the level COUNT. */
    private static long dimOnly(List<String> sql) {
        return sql.stream()
                .filter(s -> s.contains("DIM") && !s.contains("FACT_"))
                .count();
    }

    private static List<Member> query(Connection connection) {
        String mdx = """
                SELECT {[Measures].[Val]} ON COLUMNS,
                       [SharedDim].[KeyHierarchy].[KeyLevel].Members ON ROWS
                FROM [%s]
                """.formatted(SharedOrdinalCatalogSupplier.CUBE_A);
        Result result = connection.execute(connection.parseQuery(mdx));
        return result.getAxes()[1].getPositions().stream()
                .map(p -> p.get(p.size() - 1))
                .toList();
    }

    private static Connection connect(String databaseName) throws Exception {
        ActiveDatabase db = DatabaseProvider.selected().activate(databaseName);
        SharedOrdinalCatalogSupplier supplier = new SharedOrdinalCatalogSupplier();
        DatabaseLayer.apply(db.dataSource(), db.dialect(), supplier.schema());
        insertRows(db.dataSource());
        TestContext ctx = new TestContext(db.dataSource(), db.dialect(), supplier);
        return ((Context<?>) ctx).getConnectionWithDefaultRole();
    }

    private static void insertRows(DataSource dataSource) throws Exception {
        try (java.sql.Connection jdbc = dataSource.getConnection()) {
            try (PreparedStatement ps = jdbc
                    .prepareStatement("insert into \"DIM\" (\"KEY\", \"ORD\") values (?, ?)")) {
                for (int i = 0; i < 3; i++) {
                    ps.setString(1, List.of("A", "B", "C").get(i));
                    ps.setInt(2, 3 - i);
                    ps.executeUpdate();
                }
            }
            try (PreparedStatement ps = jdbc.prepareStatement(
                    "insert into \"FACT_A\" (\"KEY\", \"VAL\") values (?, ?)")) {
                for (int i = 0; i < 3; i++) {
                    ps.setString(1, List.of("A", "B", "C").get(i));
                    ps.setDouble(2, i + 1);
                    ps.executeUpdate();
                }
            }
        }
    }
}
