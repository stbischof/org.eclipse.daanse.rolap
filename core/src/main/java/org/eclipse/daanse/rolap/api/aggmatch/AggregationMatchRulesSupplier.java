/*
 * Copyright (c) 2025 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.daanse.rolap.api.aggmatch;

import java.util.function.Supplier;

/**
 * Supplier of {@link AggregationMatchRules}, used as an OSGi service type.
 * When available in the runtime, aggregate table matching is enabled.
 * When absent, no automatic aggregate table discovery occurs.
 */
public interface AggregationMatchRulesSupplier extends Supplier<AggregationMatchRules> {
}
