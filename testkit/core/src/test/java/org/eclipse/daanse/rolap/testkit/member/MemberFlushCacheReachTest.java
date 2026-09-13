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



import org.eclipse.daanse.jdbc.datasource.testkit.api.ActiveDatabase;
import org.eclipse.daanse.jdbc.datasource.testkit.api.DatabaseProvider;
import org.eclipse.daanse.cwm.testkit.database.DatabaseLayer;
import org.eclipse.daanse.olap.api.Context;
import org.eclipse.daanse.olap.api.cache.CacheControl;
import org.eclipse.daanse.olap.api.connection.Connection;
import org.eclipse.daanse.olap.api.element.Member;
import org.eclipse.daanse.olap.api.result.Result;
import org.eclipse.daanse.rolap.common.member.MemberCache;
import org.eclipse.daanse.rolap.element.RolapCubeHierarchy;
import org.eclipse.daanse.rolap.element.RolapCubeMember;
import org.eclipse.daanse.rolap.testkit.core.TestContext;
import org.junit.jupiter.api.Test;

/**
 * A member flush reaches every cache of the cube reader chain: the cube
 * reader's wrapper cache used to survive CacheControl.flush(MemberSet)
 * and keep serving the flushed member.
 */
class MemberFlushCacheReachTest {

    @Test
    void memberFlushDropsTheCubeWrapperCacheEntry() throws Exception {
        Connection connection = connect("FlushReach",
                new SharedOrdinalCatalogSupplier("on", "on", false));

        RolapCubeMember member = anyMember(connection,
                SharedOrdinalCatalogSupplier.CUBE_A,
                "[SharedDim].[KeyHierarchy].[KeyLevel].Members");

        RolapCubeHierarchy.RolapCubeHierarchyMemberReader reader =
                (RolapCubeHierarchy.RolapCubeHierarchyMemberReader)
                        member.getHierarchy().getMemberReader();
        MemberCache wrapperCache = reader.getRolapCubeMemberCache();
        Object wrapperKey = wrapperCache.makeKey(
                (org.eclipse.daanse.rolap.api.element.RolapMember) member.getParentMember(),
                member.getKey());
        assertThat(wrapperCache.getMember(wrapperKey))
                .as("the query populated the wrapper cache")
                .isNotNull();

        CacheControl cacheControl = connection.getCacheControl(null);
        cacheControl.flush(cacheControl.createMemberSet(member, false));

        assertThat(wrapperCache.getMember(wrapperKey))
                .as("flush(MemberSet) reaches the cube wrapper cache")
                .isNull();
    }

    /** First row-axis member of the given member set in the given cube. */
    private static RolapCubeMember anyMember(
            Connection connection, String cube, String memberSet) {
        String mdx = """
                SELECT {[Measures].[Val]} ON COLUMNS,
                       %s ON ROWS
                FROM [%s]
                """.formatted(memberSet, cube);
        Result result = connection.execute(connection.parseQuery(mdx));
        Member member = result.getAxes()[1].getPositions().get(0).get(0);
        return (RolapCubeMember) member;
    }

    private static Connection connect(
            String databaseName, SharedOrdinalCatalogSupplier supplier)
            throws Exception {
        ActiveDatabase db = DatabaseProvider.selected().activate(databaseName);
        DatabaseLayer.apply(db.dataSource(), db.dialect(), supplier.schema());
        SharedOrdinalCatalogSupplier.insertRows(db.dataSource());
        TestContext ctx = new TestContext(db.dataSource(), db.dialect(), supplier);
        return ((Context<?>) ctx).getConnectionWithDefaultRole();
    }
}
