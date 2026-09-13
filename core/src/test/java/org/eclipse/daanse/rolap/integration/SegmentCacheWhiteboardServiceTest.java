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
package org.eclipse.daanse.rolap.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.daanse.rolap.core.api.Constants.BASIC_CONTEXT_PID;
import static org.eclipse.daanse.rolap.core.api.Constants.BASIC_CONTEXT_REF_NAME_CATALOG_MAPPING_SUPPLIER;
import static org.eclipse.daanse.rolap.core.api.Constants.BASIC_CONTEXT_REF_NAME_CONNECTION_POOL;
import static org.eclipse.daanse.rolap.core.api.Constants.BASIC_CONTEXT_REF_NAME_DIALECT_FACTORY;
import static org.mockito.Mockito.when;
import static org.osgi.test.common.dictionary.Dictionaries.dictionaryOf;

import java.sql.Connection;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.eclipse.daanse.jdbc.datasource.pools.api.ConnectionPool;
import org.eclipse.daanse.mdx.parser.api.MdxParserProvider;
import org.eclipse.daanse.olap.api.Context;
import org.eclipse.daanse.olap.api.calc.compiler.ExpressionCompilerFactory;
import org.eclipse.daanse.olap.api.function.FunctionService;
import org.eclipse.daanse.olap.core.AbstractBasicContext;
import org.eclipse.daanse.olap.spi.SegmentBody;
import org.eclipse.daanse.olap.spi.SegmentCache;
import org.eclipse.daanse.olap.spi.SegmentHeader;
import org.eclipse.daanse.rolap.common.agg.SegmentCacheManager;
import org.eclipse.daanse.rolap.common.agg.SegmentCacheStats;
import org.eclipse.daanse.rolap.core.api.Constants;
import org.eclipse.daanse.rolap.mapping.model.catalog.CatalogFactory;
import org.eclipse.daanse.rolap.mapping.model.provider.CatalogMappingSupplier;
import org.eclipse.daanse.sql.dialect.api.Dialect;
import org.eclipse.daanse.sql.dialect.api.DialectFactory;
import org.eclipse.daanse.sql.guard.api.SqlGuardFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.Configuration;
import org.osgi.test.common.annotation.InjectBundleContext;
import org.osgi.test.common.annotation.InjectService;
import org.osgi.test.common.annotation.config.InjectConfiguration;
import org.osgi.test.common.annotation.config.WithFactoryConfiguration;
import org.osgi.test.common.service.ServiceAware;
import org.osgi.test.junit5.cm.ConfigurationExtension;
import org.osgi.test.junit5.context.BundleContextExtension;

/**
 * Runtime whiteboard behavior of BasicContext: a SegmentCache service
 * registered after activation attaches, a hidden delegate stays unbound,
 * and unregistering detaches without tearDown.
 */
@ExtendWith(BundleContextExtension.class)
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SegmentCacheWhiteboardServiceTest {

    private static final String TARGET_EXT = ".target";

    @InjectBundleContext
    BundleContext bc;

    @Mock
    Dialect dialect;
    @Mock
    DialectFactory dialectFactory;
    @Mock
    DataSource dataSource;
    @Mock
    ConnectionPool connectionPool;
    @Mock
    Connection connection;
    @Mock
    CatalogMappingSupplier catalogMappingSupplier;
    @Mock
    ExpressionCompilerFactory expressionCompilerFactory;
    @Mock
    MdxParserProvider mdxParserProvider;
    @Mock
    FunctionService functionService;
    @Mock
    SqlGuardFactory sqlGuardFactory;

    org.eclipse.daanse.rolap.mapping.model.catalog.Catalog catalogMapping = CatalogFactory.eINSTANCE.createCatalog();

    @Test
    void cacheServiceAttachesAndDetachesAtRuntime(
            @InjectConfiguration(withFactoryConfig = @WithFactoryConfiguration(factoryPid = BASIC_CONTEXT_PID, name = "cacheWb")) Configuration c,
            @InjectService(cardinality = 0) ServiceAware<Context> saContext) throws Exception {

        when(connectionPool.getConnection()).thenReturn(connection);
        when(connectionPool.dataSource()).thenReturn(dataSource);
        when(dialectFactory.createDialect(connection)).thenReturn(dialect);
        when(catalogMappingSupplier.get()).thenReturn(catalogMapping);
        catalogMapping.setName("schemaName");

        bc.registerService(ConnectionPool.class, connectionPool, dictionaryOf("ds", "wb"));
        bc.registerService(DialectFactory.class, dialectFactory, dictionaryOf("df", "wb"));
        bc.registerService(CatalogMappingSupplier.class, catalogMappingSupplier, dictionaryOf("cms", "wb"));
        bc.registerService(ExpressionCompilerFactory.class, expressionCompilerFactory, dictionaryOf("ecf", "wb"));
        bc.registerService(MdxParserProvider.class, mdxParserProvider, dictionaryOf("parser", "wb"));
        bc.registerService(FunctionService.class, functionService, dictionaryOf("fs", "wb"));
        bc.registerService(SqlGuardFactory.class, sqlGuardFactory, dictionaryOf("sg", "wb"));

        Dictionary<String, Object> props = new Hashtable<>();
        props.put(BASIC_CONTEXT_REF_NAME_CONNECTION_POOL + TARGET_EXT, "(ds=wb)");
        props.put(BASIC_CONTEXT_REF_NAME_DIALECT_FACTORY + TARGET_EXT, "(df=wb)");
        props.put(BASIC_CONTEXT_REF_NAME_CATALOG_MAPPING_SUPPLIER + TARGET_EXT, "(cms=wb)");
        props.put(Constants.BASIC_CONTEXT_REF_NAME_EXPRESSION_COMPILER_FACTORY + TARGET_EXT, "(ecf=wb)");
        c.update(props);
        Context<?> ctx = saContext.waitForService(1000);
        SegmentCacheManager manager =
                (SegmentCacheManager) ((AbstractBasicContext<?>) ctx).getAggregationManager().getSegmentCacheManager();
        int attachedBefore = manager.getCacheStats().size();

        // bind after activation
        RecordingCache cache = new RecordingCache();
        ServiceRegistration<SegmentCache> registration =
                bc.registerService(SegmentCache.class, cache, new Hashtable<>());
        awaitAttached(manager, attachedBefore + 1);
        assertThat(manager.getCacheStats()).extracting(SegmentCacheStats::cacheName).contains("RecordingCache");

        // hidden delegates stay unbound
        ServiceRegistration<SegmentCache> hidden = bc.registerService(SegmentCache.class, new RecordingCache(),
                dictionaryOf("daanse.segmentcache.hidden", "true"));
        Thread.sleep(200);
        assertThat(manager.getCacheStats()).hasSize(attachedBefore + 1);
        hidden.unregister();

        // unbind detaches without tearDown
        registration.unregister();
        awaitAttached(manager, attachedBefore);
        assertThat(cache.tornDown).isFalse();
    }

    private static void awaitAttached(SegmentCacheManager manager, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (manager.getCacheStats().size() != expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertThat(manager.getCacheStats()).hasSize(expected);
    }

    static final class RecordingCache implements SegmentCache {
        final AtomicBoolean tornDown = new AtomicBoolean();

        @Override
        public SegmentBody get(SegmentHeader header) {
            return null;
        }

        @Override
        public boolean put(SegmentHeader header, SegmentBody body) {
            return true;
        }

        @Override
        public boolean remove(SegmentHeader header) {
            return false;
        }

        @Override
        public List<SegmentHeader> getSegmentHeaders() {
            return List.of();
        }

        @Override
        public void tearDown() {
            tornDown.set(true);
        }

        @Override
        public void addListener(SegmentCacheListener listener) {
        }

        @Override
        public void removeListener(SegmentCacheListener listener) {
        }

    }
}
