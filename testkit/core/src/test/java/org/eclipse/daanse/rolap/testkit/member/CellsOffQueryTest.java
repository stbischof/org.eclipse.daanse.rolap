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
package org.eclipse.daanse.rolap.testkit.member;

import static org.assertj.core.api.Assertions.assertThat;



import org.eclipse.daanse.cwm.testkit.database.DatabaseLayer;
import org.eclipse.daanse.jdbc.datasource.testkit.api.ActiveDatabase;
import org.eclipse.daanse.jdbc.datasource.testkit.api.DatabaseProvider;
import org.eclipse.daanse.olap.api.Context;
import org.eclipse.daanse.olap.api.connection.Connection;
import org.eclipse.daanse.olap.api.result.Result;
import org.eclipse.daanse.rolap.testkit.core.TestContext;
import org.junit.jupiter.api.Test;

/**
 * A cube tagged {@code daanse:cache.cells=off} must still be queryable
 * while GLOBAL caching stays on: its segments bypass the cache, but the
 * SQL that loads them still runs and the query answers.
 */
class CellsOffQueryTest {

    @Test
    void cellsOffCubeAnswersQueries() throws Exception {
        ActiveDatabase db = DatabaseProvider.selected().activate("CellsOffQuery");
        SharedOrdinalCatalogSupplier supplier =
                new SharedOrdinalCatalogSupplier(null, null, false, null, "off");
        DatabaseLayer.apply(db.dataSource(), db.dialect(), supplier.schema());
        SharedOrdinalCatalogSupplier.insertRows(db.dataSource());
        TestContext ctx = new TestContext(db.dataSource(), db.dialect(), supplier);
        Connection connection = ((Context<?>) ctx).getConnectionWithDefaultRole();

        String mdx = """
                SELECT {[Measures].[Val]} ON COLUMNS
                FROM [%s]
                """.formatted(SharedOrdinalCatalogSupplier.CUBE_A);
        Result result = connection.execute(connection.parseQuery(mdx));

        assertThat(result.getCell(new int[] { 0 }).getValue())
                .as("the cells=off cube must answer from SQL")
                .isEqualTo(6.0);

        // and a second run answers again (nothing was cached, nothing may
        // depend on a cache entry from the first run)
        Result again = connection.execute(connection.parseQuery(mdx));
        assertThat(again.getCell(new int[] { 0 }).getValue()).isEqualTo(6.0);
    }
}
