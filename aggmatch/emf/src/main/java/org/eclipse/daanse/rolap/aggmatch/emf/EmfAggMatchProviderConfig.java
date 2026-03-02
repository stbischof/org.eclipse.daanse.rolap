/*
 * Copyright (c) 2025 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.daanse.rolap.aggmatch.emf;

import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@ObjectClassDefinition(name = "EMF AggMatch Provider", description = "Provides aggregate table matching rules loaded from an EMF/XMI resource file")
public @interface EmfAggMatchProviderConfig {

    @AttributeDefinition(name = "resource.url", description = "File path to the EMF/XMI resource containing aggregation match rules")
    String resource_url();
}
