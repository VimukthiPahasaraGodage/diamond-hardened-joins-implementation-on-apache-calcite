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

public class SchemaBuilder3 {
  private static SchemaBuilder3 schemaBuilder;
  private RelDataTypeFactory typeFactory;
  private CalciteSchema schema;

  private SchemaBuilder3() throws Exception {
    System.out.println("Loading the CSV files into ArrayLists...");
    DatabaseLoader3 databaseLoader = DatabaseLoader3.getInstance();
    System.out.println("Building the database schema...");
    buildSchema(databaseLoader);
  }

  public static SchemaBuilder3 getInstance() throws Exception {
    if (schemaBuilder == null) {
      schemaBuilder = new SchemaBuilder3();
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

  private void buildSchema(DatabaseLoader3 databaseLoader) {
    typeFactory = new JavaTypeFactoryImpl();
    schema = CalciteSchema.createRootSchema(true);

    // Products Table
    RelDataTypeFactory.Builder productsType = new RelDataTypeFactory.Builder(typeFactory);
    productsType.add("product_id", SqlTypeName.INTEGER);
    productsType.add("name", SqlTypeName.VARCHAR);
    productsType.add("category_id", SqlTypeName.INTEGER);
    productsType.add("supplier_id", SqlTypeName.INTEGER);
    productsType.add("manufacturer_id", SqlTypeName.INTEGER);
    schema.add("Products", new ListTable(productsType.build(), databaseLoader.getProducts()));

    // Categories Table
    RelDataTypeFactory.Builder categoriesType = new RelDataTypeFactory.Builder(typeFactory);
    categoriesType.add("category_id", SqlTypeName.INTEGER);
    categoriesType.add("name", SqlTypeName.VARCHAR);
    schema.add("Categories", new ListTable(categoriesType.build(), databaseLoader.getCategories()));

    // Subcategories Table
    RelDataTypeFactory.Builder subcategoriesType = new RelDataTypeFactory.Builder(typeFactory);
    subcategoriesType.add("subcategory_id", SqlTypeName.INTEGER);
    subcategoriesType.add("category_id", SqlTypeName.INTEGER);
    subcategoriesType.add("name", SqlTypeName.VARCHAR);
    schema.add("Subcategories", new ListTable(subcategoriesType.build(), databaseLoader.getSubcategories()));

    // Suppliers Table
    RelDataTypeFactory.Builder suppliersType = new RelDataTypeFactory.Builder(typeFactory);
    suppliersType.add("supplier_id", SqlTypeName.INTEGER);
    suppliersType.add("region_id", SqlTypeName.INTEGER);
    suppliersType.add("name", SqlTypeName.VARCHAR);
    schema.add("Suppliers", new ListTable(suppliersType.build(), databaseLoader.getSuppliers()));

    // Manufacturers Table
    RelDataTypeFactory.Builder manufacturersType = new RelDataTypeFactory.Builder(typeFactory);
    manufacturersType.add("manufacturer_id", SqlTypeName.INTEGER);
    manufacturersType.add("region_id", SqlTypeName.INTEGER);
    manufacturersType.add("name", SqlTypeName.VARCHAR);
    schema.add("Manufacturers", new ListTable(manufacturersType.build(), databaseLoader.getManufacturers()));

    // Regions Table
    RelDataTypeFactory.Builder regionsType = new RelDataTypeFactory.Builder(typeFactory);
    regionsType.add("region_id", SqlTypeName.INTEGER);
    regionsType.add("country_id", SqlTypeName.INTEGER);
    regionsType.add("name", SqlTypeName.VARCHAR);
    schema.add("Regions", new ListTable(regionsType.build(), databaseLoader.getRegions()));

    // Countries Table
    RelDataTypeFactory.Builder countriesType = new RelDataTypeFactory.Builder(typeFactory);
    countriesType.add("country_id", SqlTypeName.INTEGER);
    countriesType.add("name", SqlTypeName.VARCHAR);
    schema.add("Countries", new ListTable(countriesType.build(), databaseLoader.getCountries()));

    // Reviews Table
    RelDataTypeFactory.Builder reviewsType = new RelDataTypeFactory.Builder(typeFactory);
    reviewsType.add("review_id", SqlTypeName.INTEGER);
    reviewsType.add("product_id", SqlTypeName.INTEGER);
    reviewsType.add("rating", SqlTypeName.INTEGER);
    reviewsType.add("reviewer_id", SqlTypeName.INTEGER);
    schema.add("Reviews", new ListTable(reviewsType.build(), databaseLoader.getReviews()));

    // Reviewers Table
    RelDataTypeFactory.Builder reviewersType = new RelDataTypeFactory.Builder(typeFactory);
    reviewersType.add("reviewer_id", SqlTypeName.INTEGER);
    reviewersType.add("name", SqlTypeName.VARCHAR);
    reviewersType.add("region_id", SqlTypeName.INTEGER);
    schema.add("Reviewers", new ListTable(reviewersType.build(), databaseLoader.getReviewers()));

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
