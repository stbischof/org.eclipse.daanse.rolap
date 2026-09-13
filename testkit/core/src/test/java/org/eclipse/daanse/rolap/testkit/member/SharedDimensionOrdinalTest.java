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

import javax.sql.DataSource;

import org.eclipse.daanse.cwm.testkit.database.DatabaseLayer;
import org.eclipse.daanse.jdbc.datasource.testkit.api.ActiveDatabase;
import org.eclipse.daanse.jdbc.datasource.testkit.api.DatabaseProvider;
import org.eclipse.daanse.olap.api.Context;
import org.eclipse.daanse.olap.api.connection.Connection;
import org.eclipse.daanse.olap.api.element.Member;
import org.eclipse.daanse.olap.api.result.Position;
import org.eclipse.daanse.olap.api.result.Result;
import org.eclipse.daanse.rolap.testkit.core.TestContext;
import org.junit.jupiter.api.Test;

/**
 * Two cubes over ONE shared dimension whose level carries an ordinalColumn
 * (ORD reverses the alphabetical key order: C &lt; B &lt; A).
 *
 * <p>
 * Both cubes wrap the same shared members; each {@code RolapCubeMember}
 * carries a cube-local ordinal, so loading one cube must never disturb the
 * ordinal order the other cube reports — sequentially or in parallel.
 */
class SharedDimensionOrdinalTest {

    private static final List<String> ORD_ORDER = List.of("C", "B", "A");

    @Test
    void ordinalsPerCubeAreStableAcrossInterleavedQueries() throws Exception {
        Connection connection = connect("SharedOrdinalSequential");

        List<Member> firstA = levelMembers(connection, SharedOrdinalCatalogSupplier.CUBE_A);
        assertOrdOrder(firstA);
        List<Integer> firstAOrdinals = ordinals(firstA);

        List<Member> firstB = levelMembers(connection, SharedOrdinalCatalogSupplier.CUBE_B);
        assertOrdOrder(firstB);

        List<Member> secondA = levelMembers(connection, SharedOrdinalCatalogSupplier.CUBE_A);
        assertOrdOrder(secondA);
        assertThat(ordinals(secondA))
                .as("CubeA ordinals after CubeB loaded the shared dimension")
                .isEqualTo(firstAOrdinals);
    }

    @Test
    void ordinalsPerCubeHoldUnderParallelFirstLoad() throws Exception {
        Connection connection = connect("SharedOrdinalParallel");

        CompletableFuture<List<Member>> a = CompletableFuture
                .supplyAsync(() -> levelMembers(connection, SharedOrdinalCatalogSupplier.CUBE_A));
        CompletableFuture<List<Member>> b = CompletableFuture
                .supplyAsync(() -> levelMembers(connection, SharedOrdinalCatalogSupplier.CUBE_B));

        assertOrdOrder(a.join());
        assertOrdOrder(b.join());
    }

    /** Row-axis members of [SharedDim].[KeyHierarchy].[KeyLevel] in axis order. */
    private static List<Member> levelMembers(Connection connection, String cube) {
        String mdx = """
                SELECT {[Measures].[Val]} ON COLUMNS,
                       [SharedDim].[KeyHierarchy].[KeyLevel].Members ON ROWS
                FROM [%s]
                """.formatted(cube);
        Result result = connection.execute(connection.parseQuery(mdx));
        return result.getAxes()[1].getPositions().stream()
                .map(p -> (Member) p.get(p.size() - 1))
                .toList();
    }

    /** Axis order and getOrdinal order must both follow the ordinalColumn. */
    private static void assertOrdOrder(List<Member> members) {
        assertThat(members).extracting(Member::getName).containsExactlyElementsOf(ORD_ORDER);
        List<Integer> ordinals = ordinals(members);
        assertThat(ordinals).as("ordinals along the ordinalColumn order %s", ordinals).isSorted();
    }

    private static List<Integer> ordinals(List<Member> members) {
        return members.stream().map(Member::getOrdinal).toList();
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
                insertDim(ps, "A", 3);
                insertDim(ps, "B", 2);
                insertDim(ps, "C", 1);
            }
            for (String fact : List.of("FACT_A", "FACT_B")) {
                try (PreparedStatement ps = jdbc.prepareStatement(
                        "insert into \"" + fact + "\" (\"KEY\", \"VAL\") values (?, ?)")) {
                    double base = fact.endsWith("A") ? 1 : 10;
                    insertFact(ps, "A", base);
                    insertFact(ps, "B", 2 * base);
                    insertFact(ps, "C", 3 * base);
                }
            }
        }
    }

    private static void insertDim(PreparedStatement ps, String key, int ord) throws Exception {
        ps.setString(1, key);
        ps.setInt(2, ord);
        ps.executeUpdate();
    }

    private static void insertFact(PreparedStatement ps, String key, double val) throws Exception {
        ps.setString(1, key);
        ps.setDouble(2, val);
        ps.executeUpdate();
    }
}
