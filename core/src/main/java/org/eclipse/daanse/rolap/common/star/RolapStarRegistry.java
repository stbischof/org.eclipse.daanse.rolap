/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2001-2005 Julian Hyde
 * Copyright (C) 2005-2019 Hitachi Vantara and others
 * All Rights Reserved.
 *
 * For more information please visit the Project: Hitachi Vantara - Mondrian
 *
 * ---- All changes after Fork in 2023 ------------------------
 *
 * Project: Eclipse daanse
 *
 * Copyright (c) 2025 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors after Fork in 2023:
 *   SmartCity Jena - initial
 *   Stefan Bischof (bipolis.org) - initial
 */
package org.eclipse.daanse.rolap.common.star;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.daanse.olap.api.Context;
import org.eclipse.daanse.olap.api.cache.OlapSegmentCacheManager;
import org.eclipse.daanse.olap.api.connection.Connection;
import org.eclipse.daanse.olap.core.AbstractBasicContext;
import org.eclipse.daanse.rolap.common.RolapUtil;
import org.eclipse.daanse.rolap.common.agg.SegmentCacheManager;
import org.eclipse.daanse.rolap.element.RolapCatalog;

/**
 * A registry for {@link RolapStar}s of this Schema.
 */
public class RolapStarRegistry {
	private final Map<List<String>, RolapStar> stars = new HashMap<>();
	private RolapCatalog schema;
	private Context<?> context;

	public RolapStarRegistry(RolapCatalog schema, Context context) {
		this.schema = schema;
		this.context = context;
	}

	/**
	 * Looks up a {@link RolapStar}, creating it if it does not exist.
	 *
	 * {@link RolapStar.Table#addJoin} works in a similar way.
	 */
	public RolapStar getOrCreateStar(final org.eclipse.daanse.rolap.mapping.model.database.source.RelationalSource fact) {
		final List<String> rolapStarKey = RolapUtil.makeRolapStarKey(fact);
		RolapStar star;
		boolean created = false;
		synchronized (this) {
			star = stars.get(rolapStarKey);
			if (star == null) {
				star = makeRolapStar(fact);
				stars.put(rolapStarKey, star);
				created = true;
			}
		}
		if (created) {
			// priming OUTSIDE the monitor: the actor takes this monitor via
			// getStar(header) on every external store event, and priming
			// must never make it wait behind store I/O. Star setup is
			// catalog-wide and always primes the shared manager.
			Connection internalConnection = schema.getInternalConnection();
			AbstractBasicContext abc = (AbstractBasicContext) internalConnection.getContext();
			OlapSegmentCacheManager segmentCacheManager = abc.getAggregationManager().getSegmentCacheManager();
			((SegmentCacheManager) segmentCacheManager).schedulePrimingIfPending(star);
		}
		return star;
	}

	public RolapStar getStar(final String factTableName) {
		return getStar(makeRolapStarKey(factTableName));
	}

	/**
	 * Builds a star OUTSIDE the registry: not primed, not resolvable from
	 * segment headers. {@link #getOrCreateStar} is the normal path; this one
	 * serves only a fact whose alias collides with a registered star (a
	 * writeback session view).
	 */
	public RolapStar makeRolapStar(final org.eclipse.daanse.rolap.mapping.model.database.source.RelationalSource fact) {
		return new RolapStar(schema, context, fact);
	}

	public synchronized RolapStar getStar(List<String> starKey) {
		return stars.get(starKey);
	}

	/** Snapshot: callers iterate outside the lock. */
	public synchronized Collection<RolapStar> getStars() {
		return List.copyOf(stars.values());
	}

	/**
	 * Generates rolap star key based on the fact table name.
	 * 
	 * @param factTableName the fact table name based on which is generated the
	 *                      rolap star key
	 * @return the rolap star key
	 */
	public static List<String> makeRolapStarKey(String factTableName) {
		return List.of(factTableName);
	}
}