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
package org.eclipse.daanse.rolap.common.nativize;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

/**
 * The native enable switch is a live supplier: a config change flips
 * isEnabled without rebuilding the evaluator or the catalog.
 */
class RolapNativeLiveConfigTest {

    @Test
    void supplierIsConsultedOnEveryCheck() {
        AtomicBoolean flag = new AtomicBoolean(false);
        RolapNativeTopCount topCount = new RolapNativeTopCount(flag::get, 10);

        assertThat(topCount.isEnabled()).isFalse();
        flag.set(true);
        assertThat(topCount.isEnabled()).isTrue();
        flag.set(false);
        assertThat(topCount.isEnabled()).isFalse();
    }

    @Test
    void booleanSetterReplacesTheSupplier() {
        AtomicBoolean flag = new AtomicBoolean(true);
        RolapNativeTopCount topCount = new RolapNativeTopCount(flag::get, 10);

        topCount.setEnabled(false);
        assertThat(topCount.isEnabled()).isFalse();
        // the old supplier no longer feeds the switch
        flag.set(true);
        assertThat(topCount.isEnabled()).isFalse();
    }

    @Test
    void disabledSupplierShortCircuitsCreateEvaluator() {
        RolapNativeTopCount topCount = new RolapNativeTopCount(() -> false, 10);
        assertThat(topCount.createEvaluator(null, null, null, true)).isNull();
    }
}
