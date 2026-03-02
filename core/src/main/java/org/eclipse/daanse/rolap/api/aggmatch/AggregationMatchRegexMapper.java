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

import java.util.List;

/**
 * Base interface for regex-based column matchers.
 * Contains an ordered list of {@link AggregationMatchRegex} patterns that are
 * tried in sequence — a candidate column matches if any pattern matches.
 */
public interface AggregationMatchRegexMapper {

    String getId();

    List<? extends AggregationMatchRegex> getRegexes();
}
