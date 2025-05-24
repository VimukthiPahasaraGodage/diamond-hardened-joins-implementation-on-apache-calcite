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

package org.example.diamondhardenedjoins.JOB;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import com.google.common.collect.ImmutableList;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryRunner {
  static final List<RelOptRule> BASE_RULES =
      ImmutableList.of(
//          CoreRules.AGGREGATE_STAR_TABLE,
//          CoreRules.AGGREGATE_PROJECT_STAR_TABLE,
//          CalciteSystemProperty.COMMUTE.value()
//              ? CoreRules.JOIN_ASSOCIATE
//              : CoreRules.PROJECT_MERGE,
//          CoreRules.FILTER_SCAN,
//          CoreRules.PROJECT_FILTER_TRANSPOSE,
//          CoreRules.FILTER_PROJECT_TRANSPOSE,
          CoreRules.FILTER_INTO_JOIN,
//          CoreRules.JOIN_PUSH_EXPRESSIONS,
//          CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
//          CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT,
//          CoreRules.AGGREGATE_CASE_TO_FILTER,
//          CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
//          CoreRules.FILTER_AGGREGATE_TRANSPOSE,
//          CoreRules.PROJECT_WINDOW_TRANSPOSE,
//          CoreRules.MATCH,
          CoreRules.JOIN_COMMUTE,
          JoinPushThroughJoinRule.RIGHT,
          JoinPushThroughJoinRule.LEFT
//          CoreRules.SORT_PROJECT_TRANSPOSE,
//          CoreRules.SORT_JOIN_TRANSPOSE,
//          CoreRules.SORT_REMOVE_CONSTANT_KEYS,
//          CoreRules.SORT_UNION_TRANSPOSE,
//          CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS,
//          CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS,
//          CoreRules.SAMPLE_TO_FILTER,
//          CoreRules.FILTER_SAMPLE_TRANSPOSE,
//          CoreRules.FILTER_WINDOW_TRANSPOSE
      );
  static final List<RelOptRule> ABSTRACT_RULES =
      ImmutableList.of(CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS,
          CoreRules.UNION_PULL_UP_CONSTANTS,
          PruneEmptyRules.UNION_INSTANCE,
          PruneEmptyRules.INTERSECT_INSTANCE,
          PruneEmptyRules.MINUS_INSTANCE,
          PruneEmptyRules.PROJECT_INSTANCE,
          PruneEmptyRules.FILTER_INSTANCE,
          PruneEmptyRules.SORT_INSTANCE,
          PruneEmptyRules.AGGREGATE_INSTANCE,
          PruneEmptyRules.JOIN_LEFT_INSTANCE,
          PruneEmptyRules.JOIN_RIGHT_INSTANCE,
          PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
          PruneEmptyRules.EMPTY_TABLE_INSTANCE,
          SingleValuesOptimizationRules.JOIN_LEFT_INSTANCE,
          SingleValuesOptimizationRules.JOIN_RIGHT_INSTANCE,
          SingleValuesOptimizationRules.JOIN_LEFT_PROJECT_INSTANCE,
          SingleValuesOptimizationRules.JOIN_RIGHT_PROJECT_INSTANCE,
          CoreRules.UNION_MERGE,
          CoreRules.INTERSECT_MERGE,
          CoreRules.MINUS_MERGE,
          CoreRules.PROJECT_OVER_SUM_TO_SUM0_RULE,
          CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
          CoreRules.FILTER_MERGE,
          DateRangeRules.FILTER_INSTANCE,
          CoreRules.INTERSECT_TO_DISTINCT,
          CoreRules.MINUS_TO_DISTINCT);
  static final List<RelOptRule> ABSTRACT_RELATIONAL_RULES =
      ImmutableList.of(CoreRules.FILTER_INTO_JOIN,
          CoreRules.JOIN_CONDITION_PUSH,
          AbstractConverter.ExpandConversionRule.INSTANCE,
          CoreRules.JOIN_COMMUTE,
          CoreRules.PROJECT_TO_SEMI_JOIN,
          CoreRules.JOIN_ON_UNIQUE_TO_SEMI_JOIN,
          CoreRules.JOIN_TO_SEMI_JOIN,
          CoreRules.AGGREGATE_REMOVE,
          CoreRules.UNION_TO_DISTINCT,
          CoreRules.UNION_TO_VALUES,
          CoreRules.PROJECT_REMOVE,
          CoreRules.PROJECT_AGGREGATE_MERGE,
          CoreRules.AGGREGATE_JOIN_TRANSPOSE,
          CoreRules.AGGREGATE_MERGE,
          CoreRules.AGGREGATE_PROJECT_MERGE,
          CoreRules.CALC_REMOVE,
          CoreRules.SORT_REMOVE);
  private static final String queryFilesFolder = "C:\\query_results\\";
  private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath
      , viewPath) -> null;
  private static int successfulQueries = 0;
  private final SchemaBuilder schemaBuilder;

  public QueryRunner() throws Exception {
    schemaBuilder = SchemaBuilder.getInstance();
  }

  public static void appendToFile(String filename, String text, boolean printToStdOutput) {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {
      writer.write(text);
      writer.newLine(); // Adds a newline after each entry

      if (printToStdOutput) {
        System.out.println(text);
      }
    } catch (IOException e) {
      System.err.println("Error writing to file: " + e.getMessage());
    }
  }

  private static RelOptCluster newCluster(RelDataTypeFactory factory) {
    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(factory));
  }

  public static String getTimestampForFilename() {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    return LocalDateTime.now().format(formatter);
  }

  // Extracts the main argument before the first flag (used for query or filename)
  private static String extractMainArg(String input) {
    int flagStart = input.indexOf(" --");
    return (flagStart == -1 ? input : input.substring(0, flagStart)).trim();
  }

  // Extracts a boolean flag like --std-out 0|1
  private static boolean extractFlagBool(String input, String flag, boolean defaultValue) {
    Pattern pattern = Pattern.compile(flag + "\\s+(\\d+)");
    Matcher matcher = pattern.matcher(input);
    if (matcher.find()) {
      return !matcher.group(1).equals("0");
    }
    return defaultValue;
  }

  // Extracts a string value flag like --out <file>
  private static String extractFlagValue(String input, String flag, String defaultValue) {
    Pattern pattern = Pattern.compile(flag + "\\s+(\\S+)");
    Matcher matcher = pattern.matcher(input);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return defaultValue;
  }

  private static String getHorizontalDivider() {
    return
        "\n------------------------------------------------------------------------------------------------------------------\n";
  }

  public void runQuery(String sqlQuery, String outputFilename, boolean printToStdOutput,
      boolean omitExecution, String optimization) {
    try {
      appendToFile(outputFilename, getHorizontalDivider() + "[SQL Query]\n" + sqlQuery,
          printToStdOutput);
      SqlParser parser = SqlParser.create(sqlQuery);
      SqlNode sqlNode = parser.parseQuery();

      appendToFile(outputFilename, "\n[SqlNode]\n" + sqlNode, printToStdOutput);

      Properties props = new Properties();
      props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
      CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
      CalciteCatalogReader catalogReader = new CalciteCatalogReader(
          schemaBuilder.getSchema(),
          Collections.singletonList(""),
          schemaBuilder.getTypeFactory(),
          config);

      SqlValidator validator = SqlValidatorUtil.newValidator(
          SqlStdOperatorTable.instance(),
          catalogReader,
          schemaBuilder.getTypeFactory(),
          SqlValidator.Config.DEFAULT);

      SqlNode validNode = validator.validate(sqlNode);

      appendToFile(outputFilename, "\n[Valid SqlNode]\n" + validNode, printToStdOutput);

      RelOptCluster cluster = newCluster(schemaBuilder.getTypeFactory());
      SqlToRelConverter relConverter = new SqlToRelConverter(
          NOOP_EXPANDER,
          validator,
          catalogReader,
          cluster,
          StandardConvertletTable.INSTANCE,
          SqlToRelConverter.config());

      RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

      appendToFile(outputFilename, RelOptUtil.dumpPlan("\n[Logical plan]", logPlan,
          SqlExplainFormat.TEXT,
          SqlExplainLevel.EXPPLAN_ATTRIBUTES), printToStdOutput);

      RelOptPlanner planner = cluster.getPlanner();
      addRulesToPlanner(planner);

      logPlan = planner.changeTraits(logPlan,
          cluster.traitSet().replace(BindableConvention.INSTANCE));
      planner.setRoot(logPlan);
      BindableRel phyPlan = (BindableRel) planner.findBestExp();

      appendToFile(outputFilename, RelOptUtil.dumpPlan("\n[Physical plan]", phyPlan,
          SqlExplainFormat.TEXT,
          SqlExplainLevel.NON_COST_ATTRIBUTES), printToStdOutput);

      if (!omitExecution) {
        appendToFile(outputFilename, "\n[Output]", printToStdOutput);

        for (Object[] row : phyPlan.bind(new SchemaOnlyDataContext(schemaBuilder.getSchema()))) {
          appendToFile(outputFilename, Arrays.toString(row), printToStdOutput);
        }
      }
      successfulQueries++;
    } catch (Exception e) {
      appendToFile(outputFilename, "Error while executing the query: " + e.getMessage(),
          printToStdOutput);
      // e.printStackTrace();
    }
  }

  private void addRulesToPlanner(RelOptPlanner planner) {
    for (RelOptRule rule : Bindables.RULES) {
      planner.addRule(rule);
    }

    for (RelOptRule rule : BASE_RULES) {
      planner.addRule(rule);
    }

//    for (RelOptRule rule : ABSTRACT_RULES) {
//      planner.addRule(rule);
//    }

//    for (RelOptRule rule : ABSTRACT_RELATIONAL_RULES) {
//      planner.addRule(rule);
//    }
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
