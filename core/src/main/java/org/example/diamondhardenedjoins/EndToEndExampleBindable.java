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
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class EndToEndExampleBindable {
  private static final List<Object[]> BOOK_DATA = Arrays.asList(
      new Object[]{1, "Les Miserables", 1862, 0, 0},
      new Object[]{2, "The Hunchback of Notre-Dame", 1829, 0, 1},
      new Object[]{3, "The Last Day of a Condemned Man", 1829, 0, 2},
      new Object[]{4, "The three Musketeers", 1844, 1, 0},
      new Object[]{5, "The Count of Monte Cristo", 1884, 1, 1}
  );

  private static final List<Object[]> AUTHOR_DATA = Arrays.asList(
      new Object[]{0, "Victor", "Hugo"},
      new Object[]{1, "Alexandre", "Dumas"}
  );

  private static final List<Object[]> PUBLISHER_DATA = Arrays.asList(
      new Object[]{0, "Publisher 1"},
      new Object[]{1, "Publisher 2"},
      new Object[]{2, "Publisher 3"}
  );
  private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath
      , viewPath) -> null;

  private static RelOptCluster newCluster(RelDataTypeFactory factory) {
    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(factory));
  }

  public static void main(String[] args) throws Exception {
    new EndToEndExampleBindable().example1();
  }

  public void example2() throws Exception {
    // Instantiate a type factory for creating types (e.g., VARCHAR, NUMERIC, etc.)
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    // Create the root schema describing the data model
    CalciteSchema schema = CalciteSchema.createRootSchema(true);
    // Define type for authors table
    RelDataTypeFactory.Builder authorType = new RelDataTypeFactory.Builder(typeFactory);
    authorType.add("id", SqlTypeName.INTEGER);
    authorType.add("fname", SqlTypeName.VARCHAR);
    authorType.add("lname", SqlTypeName.VARCHAR);

    // Initialize authors table with data
    ListTable authorsTable = new ListTable(authorType.build(), AUTHOR_DATA);
    // Add authors table to the schema
    schema.add("author", authorsTable);
    // Define type for books table
    RelDataTypeFactory.Builder bookType = new RelDataTypeFactory.Builder(typeFactory);
    bookType.add("id", SqlTypeName.INTEGER);
    bookType.add("title", SqlTypeName.VARCHAR);
    bookType.add("year", SqlTypeName.INTEGER);
    bookType.add("author", SqlTypeName.INTEGER);
    bookType.add("publisher", SqlTypeName.INTEGER);
    // Initialize books table with data
    ListTable booksTable = new ListTable(bookType.build(), BOOK_DATA);
    // Add books table to the schema
    schema.add("book", booksTable);

    RelDataTypeFactory.Builder publisherType = new RelDataTypeFactory.Builder(typeFactory);
    publisherType.add("id", SqlTypeName.INTEGER);
    publisherType.add("name", SqlTypeName.VARCHAR);
    ListTable publisherTable = new ListTable(publisherType.build(), PUBLISHER_DATA);
    schema.add("Publisher", publisherTable);

    // Create an SQL parser
    SqlParser parser = SqlParser.create("SELECT b.title, a.fname || ' ' || a.lname, p.name FROM " +
        "Book b JOIN Author a ON b.author = a.id JOIN Publisher p ON p.id = b.publisher WHERE b" +
        ".\"year\" > 1880");
    // Parse the query into an AST
    SqlNode sqlNode = parser.parseQuery();

    // Configure and instantiate validator
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
        Collections.singletonList(""),
        typeFactory, config);

    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
        catalogReader, typeFactory,
        SqlValidator.Config.DEFAULT);

    // Validate the initial AST
    SqlNode validNode = validator.validate(sqlNode);

    // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
    RelOptCluster cluster = newCluster(typeFactory);
    SqlToRelConverter relConverter = new SqlToRelConverter(
        NOOP_EXPANDER,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config());

    // Convert the valid AST into a logical plan
    RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

    // Display the logical plan
    System.out.println(
        RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.EXPPLAN_ATTRIBUTES));

    // Initialize optimizer/planner with the necessary rules
    RelOptPlanner planner = cluster.getPlanner();
    //planner.addRule(CoreRules.FILTER_INTO_JOIN);

    // Replace Bindable rules with Enumerable rules
    planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);

    // Define the type of the output plan (change to EnumerableConvention)
    logPlan = planner.changeTraits(logPlan,
        cluster.traitSet().replace(EnumerableConvention.INSTANCE));
    planner.setRoot(logPlan);

    EnumerableRel phyPlan = (EnumerableRel) planner.findBestExp();

    // Display the physical plan
    System.out.println(
        RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.NON_COST_ATTRIBUTES));
  }

  public void example1() throws Exception {
    // Instantiate a type factory for creating types (e.g., VARCHAR, NUMERIC, etc.)
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    // Create the root schema describing the data model
    CalciteSchema schema = CalciteSchema.createRootSchema(true);
    // Define type for authors table
    RelDataTypeFactory.Builder authorType = new RelDataTypeFactory.Builder(typeFactory);
    authorType.add("id", SqlTypeName.INTEGER);
    authorType.add("fname", SqlTypeName.VARCHAR);
    authorType.add("lname", SqlTypeName.VARCHAR);

    // Initialize authors table with data
    ListTable authorsTable = new ListTable(authorType.build(), AUTHOR_DATA);
    // Add authors table to the schema
    schema.add("author", authorsTable);
    // Define type for books table
    RelDataTypeFactory.Builder bookType = new RelDataTypeFactory.Builder(typeFactory);
    bookType.add("id", SqlTypeName.INTEGER);
    bookType.add("title", SqlTypeName.VARCHAR);
    bookType.add("year", SqlTypeName.INTEGER);
    bookType.add("author", SqlTypeName.INTEGER);
    bookType.add("publisher", SqlTypeName.INTEGER);
    // Initialize books table with data
    ListTable booksTable = new ListTable(bookType.build(), BOOK_DATA);
    // Add books table to the schema
    schema.add("book", booksTable);

    RelDataTypeFactory.Builder publisherType = new RelDataTypeFactory.Builder(typeFactory);
    publisherType.add("id", SqlTypeName.INTEGER);
    publisherType.add("name", SqlTypeName.VARCHAR);
    ListTable publisherTable = new ListTable(publisherType.build(), PUBLISHER_DATA);
    schema.add("Publisher", publisherTable);


    // Create an SQL parser
//    SqlParser parser = SqlParser.create(
//        "SELECT b.id, b.title, b.\"year\", a.fname || ' ' || a.lname \n"
//            + "FROM Book b\n"
//            + "LEFT OUTER JOIN Author a ON b.author=a.id\n"
//            + "WHERE b.\"year\" > 1830\n"
//            + "ORDER BY b.id\n"
//            + "LIMIT 5");
    SqlParser parser = SqlParser.create("SELECT b.title, a.fname || ' ' || a.lname, p.name FROM " +
        "Book b JOIN Author a ON b.author = a.id JOIN Publisher p ON p.id = b.publisher WHERE b" +
        ".\"year\" > 1880");
    // Parse the query into an AST
    SqlNode sqlNode = parser.parseQuery();

    // Configure and instantiate validator
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
        Collections.singletonList(""),
        typeFactory, config);

    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
        catalogReader, typeFactory,
        SqlValidator.Config.DEFAULT);

    // Validate the initial AST
    SqlNode validNode = validator.validate(sqlNode);

    // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
    RelOptCluster cluster = newCluster(typeFactory);
    SqlToRelConverter relConverter = new SqlToRelConverter(
        NOOP_EXPANDER,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config());

    // Convert the valid AST into a logical plan
    RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

    // Display the logical plan
    System.out.println(
        RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.EXPPLAN_ATTRIBUTES));

    // Initialize optimizer/planner with the necessary rules
    RelOptPlanner planner = cluster.getPlanner();
    planner.addRule(CoreRules.FILTER_INTO_JOIN);
    planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
    planner.addRule(Bindables.BINDABLE_FILTER_RULE);
    planner.addRule(Bindables.BINDABLE_JOIN_RULE);
    planner.addRule(Bindables.BINDABLE_PROJECT_RULE);
    planner.addRule(Bindables.BINDABLE_SORT_RULE);

    // Define the type of the output plan (in this case we want a physical plan in
    // BindableConvention)
    logPlan = planner.changeTraits(logPlan,
        cluster.traitSet().replace(BindableConvention.INSTANCE));
    planner.setRoot(logPlan);
    // Start the optimization process to obtain the most efficient physical plan based on the
    // provided rule set.
    BindableRel phyPlan = (BindableRel) planner.findBestExp();

    // Display the physical plan
    System.out.println(
        RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.NON_COST_ATTRIBUTES));

    // Run the executable plan using a context simply providing access to the schema
    for (Object[] row : phyPlan.bind(new SchemaOnlyDataContext(schema))) {
      System.out.println(Arrays.toString(row));
    }
  }

  public void example() throws Exception {
    // Instantiate a type factory for creating types (e.g., VARCHAR, NUMERIC, etc.)
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    // Create the root schema describing the data model
    CalciteSchema schema = CalciteSchema.createRootSchema(false);
    // Define type for authors table
    RelDataType authorType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("fname", SqlTypeName.VARCHAR)
        .add("lname", SqlTypeName.VARCHAR)
        .build();
    // Initialize authors table with data
    ListTable authorTable = new ListTable(authorType, AUTHOR_DATA);
    // Add authors table to the schema
    schema.add("author", authorTable);
    // Define type for books table
    RelDataType bookType = typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("title", SqlTypeName.VARCHAR)
        .add("year", SqlTypeName.INTEGER)
        .add("author", SqlTypeName.INTEGER)
        .build();
    // Initialize books table with data
    ListTable bookTable = new ListTable(bookType, BOOK_DATA);
    // Add books table to the schema
    schema.add("book", bookTable);
    // Create an SQL parser
    SqlParser parser = SqlParser.create("SELECT title FROM book WHERE id > 1");
    // Parse the query into an AST
    SqlNode parseNode = parser.parseQuery();
    System.out.println(parseNode);
    // Configure and instantiate validator
    CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema, Collections.emptyList(),
        typeFactory, CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE
        , "false"));
    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
        catalogReader, typeFactory, SqlValidator.Config.DEFAULT);
    // Validate the initial AST
    SqlNode validNode = validator.validate(parseNode);
    System.out.println(validNode);
    // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
    RelOptCluster cluster = newCluster(typeFactory);
    SqlToRelConverter converter = new SqlToRelConverter(
        NOOP_EXPANDER,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config());
    // Convert the valid AST into a logical plan
    RelNode logicalPlan = converter.convertQuery(validNode, false, true).rel;
    // Display the logical plan
    System.out.println(RelOptUtil.toString(logicalPlan));
    // Initialize optimizer/planner with the necessary rules
    RelOptPlanner planner = cluster.getPlanner();
    planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
    planner.addRule(Bindables.BINDABLE_FILTER_RULE);
    planner.addRule(Bindables.BINDABLE_PROJECT_RULE);
    // Define the type of the output plan (in this case we want a physical plan in
    // BindableConvention)
    logicalPlan = planner.changeTraits(logicalPlan,
        cluster.traitSet().replace(BindableConvention.INSTANCE));
    planner.setRoot(logicalPlan);
    // Start the optimization process to obtain the most efficient physical plan based on the
    // provided rule set.
    RelNode physicalPlan = planner.findBestExp();
    BindableRel finalPlan = (BindableRel) physicalPlan;
    // Display the physical plan
    System.out.println(RelOptUtil.toString(physicalPlan));
    // Run the executable plan using a context simply providing access to the schema
    for (Object[] row : finalPlan.bind(new SchemaOnlyDataContext(schema)).asEnumerable()) {
      System.out.println(Arrays.asList(row));
    }
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

  /**
   * A simple data context only with schema information.
   */
  private static final class SchemaOnlyDataContext implements DataContext {
    private final SchemaPlus schema;

    SchemaOnlyDataContext(CalciteSchema calciteSchema) {
      this.schema = calciteSchema.plus();
    }

    @Override
    public SchemaPlus getRootSchema() {
      return schema;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
      return new JavaTypeFactoryImpl();
    }

    @Override
    public QueryProvider getQueryProvider() {
      return null;
    }

    @Override
    public Object get(final String name) {
      return null;
    }
  }
}
