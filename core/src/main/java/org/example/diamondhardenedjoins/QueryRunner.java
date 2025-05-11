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
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
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

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryRunner {
  private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath
      , viewPath) -> null;
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

  public static void main(String[] args) throws Exception {
    QueryRunner queryRunner = new QueryRunner();
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    final String DEFAULT_OUTPUT_FILENAME = System.currentTimeMillis() + "_output.txt";
    final boolean DEFAULT_STD_OUT = true;

    while (true) {
      System.out.print("sql> ");
      String line = reader.readLine();

      if (line == null || line.equalsIgnoreCase("exit")) {
        break;
      }

      if (line.startsWith("\\s ")) {
        // Pattern: \s <query> [--std-out 0|1] [--out <file>]
        String commandBody = line.substring(3).trim();
        String query = extractMainArg(commandBody);
        boolean stdOut = extractFlagBool(commandBody, "--std-out", DEFAULT_STD_OUT);
        String outFile = extractFlagValue(commandBody, "--out", DEFAULT_OUTPUT_FILENAME);

        if (!query.isEmpty()) {
          queryRunner.runQuery(query, outFile, stdOut);
        }
      } else if (line.startsWith("\\f ")) {
        // Pattern: \f <filename> [--std-out 0|1] [--out <file>]
        String commandBody = line.substring(3).trim();
        String filename = extractMainArg(commandBody);
        boolean stdOut = extractFlagBool(commandBody, "--std-out", DEFAULT_STD_OUT);
        String outFile = extractFlagValue(commandBody, "--out", DEFAULT_OUTPUT_FILENAME);

        try (BufferedReader fileReader = new BufferedReader(new FileReader(filename))) {
          StringBuilder sb = new StringBuilder();
          String fileLine;
          while ((fileLine = fileReader.readLine()) != null) {
            sb.append(fileLine).append(" ");
          }

          String[] queries = sb.toString().split(";");
          for (String q : queries) {
            String cleanedQuery = q.trim().replaceAll("\\s+", " ");
            if (!cleanedQuery.isEmpty()) {
              queryRunner.runQuery(cleanedQuery, outFile, stdOut);
            }
          }
        } catch (IOException e) {
          System.err.println("Error reading file: " + e.getMessage());
        }
      } else {
        System.out.println("Unknown command. Use '\\s <query> [--std-out 0|1] [--out file]' or " +
            "'\\f <filename> [--std-out 0|1] [--out file]'. Type 'exit' to quit.");
      }
    }
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

  public void runQuery(String sqlQuery, String outputFilename, boolean printToStdOutput) {
    try {
      SqlParser parser = SqlParser.create(sqlQuery);
      SqlNode sqlNode = parser.parseQuery();

      appendToFile(outputFilename, "[SqlNode]\n" + sqlNode, printToStdOutput);

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

      appendToFile(outputFilename, "[Valid SqlNode]\n" + validNode, printToStdOutput);

      RelOptCluster cluster = newCluster(schemaBuilder.getTypeFactory());
      SqlToRelConverter relConverter = new SqlToRelConverter(
          NOOP_EXPANDER,
          validator,
          catalogReader,
          cluster,
          StandardConvertletTable.INSTANCE,
          SqlToRelConverter.config());

      RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

      appendToFile(outputFilename, RelOptUtil.dumpPlan("[Logical plan]", logPlan,
          SqlExplainFormat.TEXT,
          SqlExplainLevel.EXPPLAN_ATTRIBUTES), printToStdOutput);

      RelOptPlanner planner = cluster.getPlanner();
      planner.addRule(CoreRules.FILTER_INTO_JOIN);
      planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
      planner.addRule(Bindables.BINDABLE_FILTER_RULE);
      planner.addRule(Bindables.BINDABLE_JOIN_RULE);
      planner.addRule(Bindables.BINDABLE_PROJECT_RULE);
      planner.addRule(Bindables.BINDABLE_SORT_RULE);

      logPlan = planner.changeTraits(logPlan,
          cluster.traitSet().replace(BindableConvention.INSTANCE));
      planner.setRoot(logPlan);
      BindableRel phyPlan = (BindableRel) planner.findBestExp();

      appendToFile(outputFilename, RelOptUtil.dumpPlan("[Physical plan]", phyPlan,
          SqlExplainFormat.TEXT,
          SqlExplainLevel.NON_COST_ATTRIBUTES), printToStdOutput);

      appendToFile(outputFilename, "[Output]\n", printToStdOutput);
      for (Object[] row : phyPlan.bind(new SchemaOnlyDataContext(schemaBuilder.getSchema()))) {
        appendToFile(outputFilename, Arrays.toString(row), printToStdOutput);
      }
    } catch (Exception e) {
      System.out.println("Error while executing the query: " + e.getMessage());
      e.printStackTrace();
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
