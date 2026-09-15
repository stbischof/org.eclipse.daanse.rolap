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
 *   SmartCity Jena, Stefan Bischof - initial
 */
package org.eclipse.daanse.rolap.testkit.core;

import org.eclipse.daanse.olap.api.function.FunctionService;

/**
 * The standard function registry, as the test kit hands it out.
 */
public final class FunctionServices {

    private FunctionServices() {
    }

    public static FunctionService standard() {
        FunctionService svc = org.eclipse.daanse.olap.function.services.standard.StandardFunctions.standard();
        return svc;
    }
}
