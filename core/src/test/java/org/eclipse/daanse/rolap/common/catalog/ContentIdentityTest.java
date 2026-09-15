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
package org.eclipse.daanse.rolap.common.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ContentIdentityTest {

    private static final byte[] MAPPING = new byte[] { 1, 2, 3 };
    private static final String SERVER_URL = "jdbc:postgresql://db.example:5432/sales";

    @Test
    void sameInputsShareTheIdentity() {
        assertThat(ContentIdentity.sha256(MAPPING, SERVER_URL, "app", "sales", "public"))
                .isEqualTo(ContentIdentity.sha256(MAPPING, SERVER_URL, "app", "sales", "public"));
    }

    @Test
    void urlUserCatalogSchemaAndMappingEachSeparateTheIdentity() {
        byte[] base = ContentIdentity.sha256(MAPPING, SERVER_URL, "app", "sales", "public");

        assertThat(ContentIdentity.sha256(MAPPING, "jdbc:postgresql://other:5432/sales", "app", "sales", "public"))
                .isNotEqualTo(base);
        assertThat(ContentIdentity.sha256(MAPPING, SERVER_URL, "restricted", "sales", "public")).isNotEqualTo(base);
        assertThat(ContentIdentity.sha256(MAPPING, SERVER_URL, "app", "hr", "public")).isNotEqualTo(base);
        assertThat(ContentIdentity.sha256(MAPPING, SERVER_URL, "app", "sales", "audit")).isNotEqualTo(base);
        assertThat(ContentIdentity.sha256(new byte[] { 9 }, SERVER_URL, "app", "sales", "public")).isNotEqualTo(base);
    }

    @Test
    void fieldValuesCannotShiftIntoEachOther() {
        assertThat(ContentIdentity.sha256(MAPPING, "u", "ab", "c", "d"))
                .isNotEqualTo(ContentIdentity.sha256(MAPPING, "u", "a", "bc", "d"));
    }

    @Test
    void embeddedUrlsAreNodeLocal() {
        assertThat(ContentIdentity.isNodeLocal("jdbc:h2:mem:test")).isTrue();
        assertThat(ContentIdentity.isNodeLocal("jdbc:duckdb:")).isTrue();
        assertThat(ContentIdentity.isNodeLocal("jdbc:sqlite:/tmp/x.db")).isTrue();
        assertThat(ContentIdentity.isNodeLocal(null)).isTrue();
        assertThat(ContentIdentity.isNodeLocal(SERVER_URL)).isFalse();
        assertThat(ContentIdentity.isNodeLocal("jdbc:h2:tcp://host/db")).isFalse();
    }

    @Test
    void nodeScopeSeparatesIdentitiesOfEqualEmbeddedUrls() {
        String url = "jdbc:h2:mem:test";
        byte[] vm1 = ContentIdentity.sha256(MAPPING, url, "sa", null, "PUBLIC", "vm-1");
        byte[] vm2 = ContentIdentity.sha256(MAPPING, url, "sa", null, "PUBLIC", "vm-2");
        assertThat(vm1).isNotEqualTo(vm2);
    }
}
