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

import org.example.diamondhardenedjoins.custom_benchmark2.QueryRunner;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DatabaseEngine {
  private static final boolean DEFAULT_STD_OUT = true;
  private static final boolean DEFAULT_EXEC_CHOICE = true;
  private static final String DEFAULT_OPT_METHOD = "normal";
  private static final String DEFAULT_EXECUTION_TREE_VISUALIZATIONS_FOLDER =
      "visualization_outputs";
  private static final String DEFAULT_GENERATED_CODES_FOLDER = "generated_codes";
  private static final String DEFAULT_LOGS_FOLDER = "execution_outputs";
  private static final String DEFAULT_QUERY_FILES_FOLDER = "C:\\calcite_outputs\\";
  private static final String DEFAULT_BACKEND_EXECUTABLE_PATH = "C:\\execution_backend\\driver.exe";
  private static final String DEFAULT_DATASET_CSV_PATH = "C:\\\\Benchmark2_Dataset";

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

  public static void main(String[] args) throws Exception {
    QueryRunner queryRunner = new QueryRunner();
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));


    while (true) {
      System.out.print("sql> ");
      String line = reader.readLine();

      if (line.trim().isEmpty() || line.equalsIgnoreCase("exit")) {
        continue;
      }

      final String DEFAULT_OUTPUT_FILENAME = getTimestampForFilename() + "_output.txt";
      QueryRunner.setSuccessfulQueries(0);

      // Pattern: (\s or \f) <query> [--std-out 0|1] [--out <file>]
      String commandBody = line.substring(3).trim();
      String mainArg = extractMainArg(commandBody);
      boolean stdOut = extractFlagBool(commandBody, "--std-out", DEFAULT_STD_OUT);
      boolean execChoice = extractFlagBool(commandBody, "--omit-exec", DEFAULT_EXEC_CHOICE);
      String outFile = extractFlagValue(commandBody, "--out", DEFAULT_OUTPUT_FILENAME);
      String optimizationMethod = extractFlagValue(commandBody, "--opt", DEFAULT_OPT_METHOD);
      String executionTreeVisualizationFolder = extractFlagValue(commandBody, "--execution-tree" +
          "-visualizations-folder", DEFAULT_EXECUTION_TREE_VISUALIZATIONS_FOLDER);
      String generatedCodesFolder = extractFlagValue(commandBody, "--generated-codes-folder",
          DEFAULT_GENERATED_CODES_FOLDER);
      String logsFolder = extractFlagValue(commandBody, "--logs-folder", DEFAULT_LOGS_FOLDER);
      String queryFilesFolder = extractFlagValue(commandBody, "--calcite-outputs-folder",
          DEFAULT_QUERY_FILES_FOLDER);
      String backendExecutablePath = extractFlagValue(commandBody, "--backend-exe-path",
          DEFAULT_BACKEND_EXECUTABLE_PATH);
      String csvDatasetPath = extractFlagValue(commandBody, "--csv-dataset-path",
          DEFAULT_DATASET_CSV_PATH);
      int visualize = Integer.parseInt(extractFlagValue(commandBody, "--visualize", "1"));
      int stdCodeOut = Integer.parseInt(extractFlagValue(commandBody, "--std-code-out", "0"));

      if (line.startsWith("\\s ")) {
        if (!mainArg.isEmpty()) {
          queryRunner.runQuery(mainArg, queryFilesFolder + outFile, stdOut, execChoice,
              optimizationMethod);
          appendToFile(queryFilesFolder + outFile,
              getHorizontalDivider() + QueryRunner.getSuccessfulQueries() +
              " out of 1 queries successful", stdOut);
          executeOptimizedQueryPlan(backendExecutablePath, queryFilesFolder + outFile,
              csvDatasetPath, executionTreeVisualizationFolder, generatedCodesFolder, logsFolder,
              visualize, stdCodeOut, optimizationMethod);
        }
      } else if (line.startsWith("\\f ")) {
        try (BufferedReader fileReader = new BufferedReader(new FileReader(mainArg))) {
          StringBuilder sb = new StringBuilder();
          String fileLine;
          while ((fileLine = fileReader.readLine()) != null) {
            sb.append(fileLine).append(" ");
          }

          String[] queries = sb.toString().split(";");
          int numQueriesInFile = 0;
          for (String q : queries) {
            String cleanedQuery = q.trim().replaceAll("\\s+", " ");
            if (!cleanedQuery.isEmpty()) {
              numQueriesInFile++;
              queryRunner.runQuery(cleanedQuery, queryFilesFolder + outFile, stdOut, execChoice,
                  optimizationMethod);
              executeOptimizedQueryPlan(backendExecutablePath, queryFilesFolder + outFile,
                  csvDatasetPath, executionTreeVisualizationFolder, generatedCodesFolder,
                  logsFolder, visualize, stdCodeOut, optimizationMethod);
            }
          }
          appendToFile(queryFilesFolder + outFile,
              getHorizontalDivider() + QueryRunner.getSuccessfulQueries() +
              " out of " + numQueriesInFile + " queries successful", stdOut);
        } catch (IOException e) {
          System.err.println("Error reading file: " + e.getMessage());
        }
      } else {
        System.out.println("Unknown command. Use '\\s <query> [--std-out 0|1] [--omit-exec 0|1] " +
            "[--out file]' or " +
            "'\\f <filename> [--std-out 0|1] [--omit-exec 0|1] [--out file]'. Type 'exit' to quit" +
            ".");
      }
    }
  }

  private static void executeOptimizedQueryPlan(String backendExecutablePath,
      String calciteOutputFile, String csvDatasetPath, String executionTreeVisualizationsFolder,
      String generatedCodesFolder, String logsFolder, int visualize, int std_out_code, String optimizationMethod) {
    try {
      ProcessBuilder builder = new ProcessBuilder(backendExecutablePath,
          "--calcite_output_file", calciteOutputFile,
          "--execution_tree_visualizations_folder", executionTreeVisualizationsFolder,
          "--generated_codes_folder", generatedCodesFolder,
          "--logs_folder", logsFolder,
          "--csv_dataset_path", csvDatasetPath,
          "--visualize", Integer.toString(visualize),
          "--std_out_code", Integer.toString(std_out_code),
          "--opt", optimizationMethod);

      builder.redirectErrorStream(true);

      Process process = builder.start();

      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
      }

      int exitCode = process.waitFor();
      System.out.println("Query execution backend process exited with code: " + exitCode);
    } catch (Exception e) {
      System.out.println("Exception occurred while executing the optimized query plan on " +
          "execution backend! Error: " + e.getMessage());
    }
  }

  private static String getHorizontalDivider() {
    return
        "\n------------------------------------------------------------------------------------------------------------------\n";
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
}
