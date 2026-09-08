/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2005-2005 Julian Hyde
 * Copyright (C) 2005-2017 Hitachi Vantara
 * All Rights Reserved.
 * ---- All changes after Fork in 2023 ------------------------
 *
 * Project: Eclipse daanse
 *
 * Copyright (c) 2023 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors after Fork in 2023:
 *   SmartCity Jena - initial
 */

package org.eclipse.daanse.rolap.common.agg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.eclipse.daanse.sql.dialect.api.Dialect;
import org.eclipse.daanse.sql.model.type.BestFitColumnType;
import org.eclipse.daanse.olap.api.element.OlapElement;
import org.eclipse.daanse.rolap.common.sql.QueryRecorder;
import org.eclipse.daanse.rolap.common.sqlbuild.AggregateSqlMapper;
import org.eclipse.daanse.rolap.common.star.RolapStar;
import org.eclipse.daanse.rolap.common.star.StarPredicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class DrillThroughQuerySpecTest {

  private static DrillThroughCellRequest requestMock;
  private static StarPredicate starPredicateMock;
  private static QueryRecorder sqlQueryMock;
  private static DrillThroughQuerySpec drillThroughQuerySpec;
  private static RolapStar.Column includedColumn;
  private static RolapStar.Column excludedColumn;

    @BeforeEach void beforeAll() throws Exception {

    requestMock = mock(DrillThroughCellRequest.class);
    starPredicateMock = mock(StarPredicate.class);
    sqlQueryMock = mock(QueryRecorder.class);
    RolapStar.Measure measureMock = mock(RolapStar.Measure.class);
    includedColumn = mock(RolapStar.Column.class);
    excludedColumn = mock(RolapStar.Column.class);
    RolapStar starMock = mock(RolapStar.class);

    when(requestMock.includeInSelect(any(RolapStar.Column.class)))
      .thenReturn(true);
    when(requestMock.getMeasure()).thenReturn(measureMock);
    when(requestMock.getConstrainedColumns())
      .thenReturn(new RolapStar.Column[0]);
    when(measureMock.getStar()).thenReturn(starMock);
    when(starMock.getDialect()).thenReturn(mock(Dialect.class));
    when(starPredicateMock.getConstrainedColumnList())
      .thenReturn(Collections.singletonList(includedColumn));
    when(includedColumn.getTable()).thenReturn(mock(RolapStar.Table.class));
    when(excludedColumn.getTable()).thenReturn(mock(RolapStar.Table.class));
    drillThroughQuerySpec =
      new DrillThroughQuerySpec
        (requestMock, starPredicateMock, new ArrayList<OlapElement> (), false);
  }

  // ---- appendPredicateColumns: the SELECT rule the legacy extraPredicates applied to a slicer
  // predicate's constrained columns, now feeding AggregateSqlMapper.drillThrough ----

  private static List<AggregateSqlMapper.DrillColumn> appended() {
    List<AggregateSqlMapper.DrillColumn> drillColumns = new ArrayList<>();
    drillThroughQuerySpec.appendPredicateColumns(
        starPredicateMock, drillColumns, new ArrayList<>(), new java.util.HashSet<>(), true);
    return drillColumns;
  }

  private static long selectedCount(List<AggregateSqlMapper.DrillColumn> drillColumns) {
    return drillColumns.stream().filter(AggregateSqlMapper.DrillColumn::selected).count();
  }

  @Test
  void emptyColumns() {
    when(starPredicateMock.getConstrainedColumnList())
      .thenReturn(Collections.emptyList());
    assertEquals(0, appended().size());
  }

  @Test
  void oneColumnExists() {
    List<AggregateSqlMapper.DrillColumn> drillColumns = appended();
    assertEquals(1, selectedCount(drillColumns));
    assertNotNull(drillColumns.get(0).alias());
  }

  @Test
  void twoColumnsExist() {
    when(starPredicateMock.getConstrainedColumnList())
      .thenReturn(Arrays.asList(includedColumn, excludedColumn));
    assertEquals(2, selectedCount(appended()));
  }

  @Test
  void columnsNotIncludedInSelect() {
    when(requestMock.includeInSelect(any(RolapStar.Column.class))).thenReturn(false);
    assertEquals(0, selectedCount(appended()));

    when(starPredicateMock.getConstrainedColumnList())
      .thenReturn(Arrays.asList(includedColumn, excludedColumn));
    List<AggregateSqlMapper.DrillColumn> drillColumns = appended();
    // the columns still join, they are just not projected
    assertEquals(2, drillColumns.size());
    assertEquals(0, selectedCount(drillColumns));
  }

  @Test
  void columnsPartiallyIncludedInSelect() {
    when(requestMock.includeInSelect(excludedColumn)).thenReturn(false);
    when(requestMock.includeInSelect(includedColumn)).thenReturn(true);
    when(starPredicateMock.getConstrainedColumnList())
      .thenReturn(Arrays.asList(includedColumn, excludedColumn));
    assertEquals(1, selectedCount(appended()));
  }

  // ---- translateOrResidual: the segment path's residual fallback, mirrored for drill-through ----

  /** A translatable shape takes the plain StarPredicateTranslator path. */
  @Test
  void translateOrResidualKeepsTranslatorPathForValuePredicate() {
    RolapStar.Table table = mock(RolapStar.Table.class);
    when(table.getAlias()).thenReturn("schul_jahr");
    RolapStar.Column col = mock(RolapStar.Column.class);
    when(col.getTable()).thenReturn(table);
    when(col.getExpression())
      .thenReturn(new org.eclipse.daanse.rolap.element.RolapColumn("schul_jahr", "id"));
    when(col.getDatatype())
      .thenReturn(org.eclipse.daanse.sql.model.type.Datatype.INTEGER);
    ValueColumnPredicate value = mock(ValueColumnPredicate.class);
    when(value.getConstrainedColumn()).thenReturn(col);
    when(value.getValue()).thenReturn(4);

    org.eclipse.daanse.sql.statement.api.expression.Predicate p =
      DrillThroughQuerySpec.translateOrResidual(value, mock(Dialect.class));

    org.assertj.core.api.Assertions.assertThat(p).isEqualTo(
      org.eclipse.daanse.rolap.common.sqlbuild.StarPredicateTranslator.toPredicate(value));
  }

  /** An always-true predicate adds no restriction — returns null. */
  @Test
  void translateOrResidualSkipsAlwaysTruePredicate() {
    org.assertj.core.api.Assertions.assertThat(
        DrillThroughQuerySpec.translateOrResidual(
            org.eclipse.daanse.rolap.common.agg.LiteralStarPredicate.TRUE, mock(Dialect.class)))
      .isNull();
  }
}
