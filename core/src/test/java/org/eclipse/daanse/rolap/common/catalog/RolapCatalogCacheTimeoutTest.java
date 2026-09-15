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

import java.time.Duration;

import org.eclipse.daanse.cwm.model.cwm.objectmodel.core.CoreFactory;
import org.eclipse.daanse.cwm.model.cwm.objectmodel.core.TaggedValue;
import org.eclipse.daanse.olap.api.connection.ConnectionProps;
import org.eclipse.daanse.rolap.mapping.model.catalog.Catalog;
import org.eclipse.daanse.rolap.mapping.model.catalog.CatalogFactory;
import org.junit.jupiter.api.Test;

class RolapCatalogCacheTimeoutTest {

    private static Catalog catalog(String timeoutValue) {
        Catalog catalog = CatalogFactory.eINSTANCE.createCatalog();
        catalog.setName("K");
        if (timeoutValue != null) {
            TaggedValue tag = CoreFactory.eINSTANCE.createTaggedValue();
            tag.setTag("daanse:cache.timeout");
            tag.setValue(timeoutValue);
            catalog.getTaggedValue().add(tag);
        }
        return catalog;
    }

    @Test
    void withoutTagTheConnectionDefaultApplies() {
        assertThat(RolapCatalogCache.timeout(catalog(null), new ConnectionProps()))
                .isEqualTo(new ConnectionProps().pinCatalogTimeout());
    }

    @Test
    void iso8601TagBeatsTheConnectionDefault() {
        assertThat(RolapCatalogCache.timeout(catalog("PT10M"), new ConnectionProps()))
                .isEqualTo(Duration.ofMinutes(10));
    }

    @Test
    void unparsableTagFallsBackToTheConnectionDefault() {
        assertThat(RolapCatalogCache.timeout(catalog("later"), new ConnectionProps()))
                .isEqualTo(new ConnectionProps().pinCatalogTimeout());
    }
}
