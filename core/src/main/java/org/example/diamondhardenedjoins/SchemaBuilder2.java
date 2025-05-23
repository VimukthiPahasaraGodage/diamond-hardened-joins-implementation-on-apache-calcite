/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example.diamondhardenedjoins;

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class SchemaBuilder2 {
  private static SchemaBuilder2 schemaBuilder;
  private RelDataTypeFactory typeFactory;
  private CalciteSchema schema;

  private SchemaBuilder2() throws Exception {
    System.out.println("Loading the CSV files into ArrayLists...");
    DatabaseLoader2 databaseLoader = DatabaseLoader2.getInstance();
    System.out.println("Building the database schema...");
    buildSchema(databaseLoader);
  }

  public static SchemaBuilder2 getInstance() throws Exception {
    if (schemaBuilder == null) {
      schemaBuilder = new SchemaBuilder2();
      return schemaBuilder;
    }
    return schemaBuilder;
  }

  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public CalciteSchema getSchema() {
    return schema;
  }

  private void buildSchema(DatabaseLoader2 databaseLoader) {
    typeFactory = new JavaTypeFactoryImpl();
    schema = CalciteSchema.createRootSchema(true);

    RelDataTypeFactory.Builder A_type = new RelDataTypeFactory.Builder(typeFactory);
    A_type.add("id", SqlTypeName.INTEGER);
    A_type.add("value_a", SqlTypeName.INTEGER);

    ListTable A_table = new ListTable(A_type.build(), databaseLoader.getA());
    schema.add("A", A_table);

    // =============================================================================================

    RelDataTypeFactory.Builder B_type = new RelDataTypeFactory.Builder(typeFactory);
    B_type.add("id", SqlTypeName.INTEGER);
    B_type.add("a_id", SqlTypeName.INTEGER);
    B_type.add("value_b", SqlTypeName.INTEGER);

    ListTable B_table = new ListTable(B_type.build(), databaseLoader.getB());
    schema.add("B", B_table);

    // =============================================================================================

    RelDataTypeFactory.Builder C_type = new RelDataTypeFactory.Builder(typeFactory);
    C_type.add("id", SqlTypeName.INTEGER);
    C_type.add("a_id", SqlTypeName.INTEGER);
    C_type.add("value_c", SqlTypeName.INTEGER);

    ListTable C_table = new ListTable(C_type.build(), databaseLoader.getC());
    schema.add("C", C_table);

    // =============================================================================================

    RelDataTypeFactory.Builder D_type = new RelDataTypeFactory.Builder(typeFactory);
    D_type.add("id", SqlTypeName.INTEGER);
    D_type.add("b_id", SqlTypeName.INTEGER);
    D_type.add("c_id", SqlTypeName.INTEGER);
    D_type.add("value_d", SqlTypeName.INTEGER);

    ListTable D_table = new ListTable(D_type.build(), databaseLoader.getD());
    schema.add("D", D_table);

    // =============================================================================================

    RelDataTypeFactory.Builder E_type = new RelDataTypeFactory.Builder(typeFactory);
    E_type.add("id", SqlTypeName.INTEGER);
    E_type.add("b_id", SqlTypeName.INTEGER);
    E_type.add("c_id", SqlTypeName.INTEGER);
    E_type.add("value_e", SqlTypeName.INTEGER);

    ListTable E_table = new ListTable(E_type.build(), databaseLoader.getE());
    schema.add("E", E_table);

    // =============================================================================================

    RelDataTypeFactory.Builder F_type = new RelDataTypeFactory.Builder(typeFactory);
    F_type.add("id", SqlTypeName.INTEGER);
    F_type.add("d_id", SqlTypeName.INTEGER);
    F_type.add("e_id", SqlTypeName.INTEGER);
    F_type.add("value_f", SqlTypeName.INTEGER);

    ListTable F_table = new ListTable(F_type.build(), databaseLoader.getF());
    schema.add("F", F_table);
  }

  /**
   * A simple table based on a list.
   */
  private static class ListTable extends AbstractTable implements ScannableTable {
    private final RelDataType rowType;
    private final List<Object[]> data;

    ListTable(RelDataType rowType, List<Object[]> data) {
      this.rowType = rowType;
      this.data = data;
    }

    @Override
    public Enumerable<Object[]> scan(final DataContext root) {
      return Linq4j.asEnumerable(data);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
      return rowType;
    }
  }
}
