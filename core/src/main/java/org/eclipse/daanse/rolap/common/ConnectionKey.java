/*
 * Copyright (c) 2025 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *   SmartCity Jena - initial
 *   Stefan Bischof (bipolis.org) - initial
 */
package org.eclipse.daanse.rolap.common;

import java.util.UUID;
import java.util.WeakHashMap;

import javax.sql.DataSource;

/**
 * Session-scoped part of the catalog pool key. Each DataSource instance
 * gets a process-local UUID — collision-free across live pools and GC
 * reuse; cross-instance identity comes from the content fingerprint in
 * {@code CatalogContentKey}. The aggregate scan properties are part
 * of the key because they filter the aggregate-table scan and thereby
 * shape the AggStar set of the loaded catalog.
 */
public record ConnectionKey(UUID dataSourceId, String sessionId,
		String aggregateScanSchema, String aggregateScanCatalog) {

	private static final WeakHashMap<DataSource, UUID> IDS = new WeakHashMap<>();

	public static ConnectionKey of(DataSource dataSource, String sessionId,
			String aggregateScanSchema, String aggregateScanCatalog) {
		final UUID id;
		synchronized (IDS) {
			id = IDS.computeIfAbsent(dataSource, ds -> UUID.randomUUID());
		}
		return new ConnectionKey(id, sessionId, aggregateScanSchema, aggregateScanCatalog);
	}

}
