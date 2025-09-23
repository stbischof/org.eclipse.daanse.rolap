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
package org.eclipse.daanse.rolap.common.connection;

import org.eclipse.daanse.olap.api.access.Role;
import org.eclipse.daanse.olap.api.connection.ConnectionProps;
import org.eclipse.daanse.rolap.api.RolapContext;
import org.eclipse.daanse.rolap.element.RolapCatalog;

public class InternalRolapConnection extends AbstractRolapConnection {

    public InternalRolapConnection(RolapContext context, RolapCatalog catalog, ConnectionProps connectionProps) {
        super(context, catalog, connectionProps);
        Role roleInner = this.catalog.getDefaultRole();
        setRole( roleInner );
    }
}
