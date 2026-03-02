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

import java.io.IOException;
import java.util.Map;

import org.eclipse.daanse.rolap.aggmatch.aggmatch.AggMatchPackage;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRules;
import org.eclipse.daanse.rolap.api.aggmatch.AggregationMatchRulesSupplier;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.fennec.emf.osgi.constants.EMFNamespaces;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;
import org.osgi.service.metatype.annotations.Designate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(service = AggregationMatchRulesSupplier.class, scope = ServiceScope.SINGLETON, configurationPid = "org.eclipse.daanse.rolap.aggmatch.emf.EmfAggMatchProvider")
@Designate(factory = true, ocd = EmfAggMatchProviderConfig.class)
public class EmfAggMatchProvider implements AggregationMatchRulesSupplier {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmfAggMatchProvider.class);

    @Reference(target = "(" + EMFNamespaces.EMF_MODEL_NSURI + "=" + AggMatchPackage.eNS_URI + ")")
    private ResourceSet resourceSet;

    private volatile AggregationMatchRules rules;

    @Activate
    public void activate(EmfAggMatchProviderConfig config) throws IOException {
        String url = config.resource_url();
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("resource_url must not be null or blank");
        }
        LOGGER.info("Loading aggmatch rules from: {}", url);

        URI uri = URI.createFileURI(url);
        Resource resource = resourceSet.getResource(uri, true);
        resource.load(Map.of());
        EcoreUtil.resolveAll(resource);
        EList<EObject> contents = resource.getContents();

        for (EObject eObject : contents) {
            if (eObject instanceof org.eclipse.daanse.rolap.aggmatch.aggmatch.AggregationMatchRules emfRules) {
                this.rules = EmfAggMatchConverter.convert(emfRules);
                LOGGER.info("Loaded {} aggregation match rules", this.rules.getAggregationRules().size());
                return;
            }
        }

        throw new IllegalStateException("No AggregationMatchRules found in resource: " + url);
    }

    @Deactivate
    public void deactivate() {
        resourceSet.getResources().forEach(Resource::unload);
    }

    @Override
    public AggregationMatchRules get() {
        if (rules == null) {
            throw new IllegalStateException("AggregationMatchRules not initialized");
        }
        return rules;
    }
}
