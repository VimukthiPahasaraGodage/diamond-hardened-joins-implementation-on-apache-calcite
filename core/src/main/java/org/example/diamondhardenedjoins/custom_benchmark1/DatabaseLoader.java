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

package org.example.diamondhardenedjoins.custom_benchmark1;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DatabaseLoader {
  private static DatabaseLoader databaseLoader;
  private final List<String> tables = new ArrayList<>(Arrays.asList(
      "A",
      "B",
      "C",
      "D",
      "E",
      "F"
  ));
  private final List<Object[]> A = new ArrayList<>();
  private final List<Object[]> B = new ArrayList<>();
  private final List<Object[]> C = new ArrayList<>();
  private final List<Object[]> D = new ArrayList<>();
  private final List<Object[]> E = new ArrayList<>();
  private final List<Object[]> F = new ArrayList<>();
  private int nullInt = -1;
  private double nullDouble = -1;

  private DatabaseLoader() throws Exception {
    String csvFilePath = "C:\\Benchmark_Dataset";
    if (!checkAllCsvFilesExists(csvFilePath)) {
      throw new Exception("Check for necessary CSV files failed. One or more necessary CSV files " +
          "are missing");
    }
    loadDataToArrayLists(csvFilePath);
    printSizesOfArrayLists();
  }

  public static DatabaseLoader getInstance() throws Exception {
    if (databaseLoader == null) {
      databaseLoader = new DatabaseLoader();
      return databaseLoader;
    }
    return databaseLoader;
  }

  private void printSizesOfArrayLists() {
    System.out.println("A: " + A.size());
    System.out.println("B: " + B.size());
    System.out.println("C: " + C.size());
    System.out.println("D: " + D.size());
    System.out.println("E: " + E.size());
    System.out.println("F: " + F.size());
  }

  private void loadDataToA(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\A.csv"))
        .withCSVParser(new CSVParserBuilder().withSeparator(',').build())
        .build()) {
      String[] row;
      int lineNumber = 0;
      int totalLinesSkipped = 0;

      while ((row = reader.readNext()) != null) {
        lineNumber++;
        try {
          Object[] newRow = new Object[]{
              parseNonNullableInt(row[0]),
              parseNonNullableInt(row[1])
          };
          A.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in A.csv. Line number: " + lineNumber + " " +
                  "Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to A");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to A" + e.getMessage());
    }
  }

  private void loadDataToB(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\B.csv"))
        .withCSVParser(new CSVParserBuilder().withSeparator(',').build())
        .build()) {
      String[] row;
      int lineNumber = 0;
      int totalLinesSkipped = 0;

      while ((row = reader.readNext()) != null) {
        lineNumber++;
        try {
          Object[] newRow = new Object[]{
              parseNonNullableInt(row[0]),
              parseNonNullableInt(row[1]),
              parseNonNullableInt(row[2])
          };
          B.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in B.csv. Line number: " + lineNumber + " " +
                  "Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to B");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to B" + e.getMessage());
    }
  }

  private void loadDataToC(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\C.csv"))
        .withCSVParser(new CSVParserBuilder().withSeparator(',').build())
        .build()) {
      String[] row;
      int lineNumber = 0;
      int totalLinesSkipped = 0;

      while ((row = reader.readNext()) != null) {
        lineNumber++;
        try {
          Object[] newRow = new Object[]{
              parseNonNullableInt(row[0]),
              parseNonNullableInt(row[1]),
              parseNonNullableInt(row[2])
          };
          C.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in C.csv. Line number: " + lineNumber + " " +
                  "Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to C");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to C" + e.getMessage());
    }
  }

  private void loadDataToD(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\D.csv"))
        .withCSVParser(new CSVParserBuilder().withSeparator(',').build())
        .build()) {
      String[] row;
      int lineNumber = 0;
      int totalLinesSkipped = 0;

      while ((row = reader.readNext()) != null) {
        lineNumber++;
        try {
          Object[] newRow = new Object[]{
              parseNonNullableInt(row[0]),
              parseNonNullableInt(row[1]),
              parseNonNullableInt(row[2]),
              parseNonNullableInt(row[3])
          };
          D.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in D.csv. Line number: " + lineNumber + " " +
                  "Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to D");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to D" + e.getMessage());
    }
  }

  private void loadDataToE(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\E.csv"))
        .withCSVParser(new CSVParserBuilder().withSeparator(',').build())
        .build()) {
      String[] row;
      int lineNumber = 0;
      int totalLinesSkipped = 0;

      while ((row = reader.readNext()) != null) {
        lineNumber++;
        try {
          Object[] newRow = new Object[]{
              parseNonNullableInt(row[0]),
              parseNonNullableInt(row[1]),
              parseNonNullableInt(row[2]),
              parseNonNullableInt(row[3])
          };
          E.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in E.csv. Line number: " + lineNumber + " " +
                  "Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to E");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to E" + e.getMessage());
    }
  }

  private void loadDataToF(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\F.csv"))
        .withCSVParser(new CSVParserBuilder().withSeparator(',').build())
        .build()) {
      String[] row;
      int lineNumber = 0;
      int totalLinesSkipped = 0;

      while ((row = reader.readNext()) != null) {
        lineNumber++;
        try {
          Object[] newRow = new Object[]{
              parseNonNullableInt(row[0]),
              parseNonNullableInt(row[1]),
              parseNonNullableInt(row[2]),
              parseNonNullableInt(row[3])
          };
          F.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in F.csv. Line number: " + lineNumber + " " +
                  "Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to F");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to F" + e.getMessage());
    }
  }

  private void loadDataToArrayLists(String csvFilePath) {
    ExecutorService executor = Executors.newFixedThreadPool(21);

    List<Callable<Void>> tasks = Arrays.asList(
        () -> {
          loadDataToA(csvFilePath);
          return null;
        },
        () -> {
          loadDataToB(csvFilePath);
          return null;
        },
        () -> {
          loadDataToC(csvFilePath);
          return null;
        },
        () -> {
          loadDataToD(csvFilePath);
          return null;
        },
        () -> {
          loadDataToE(csvFilePath);
          return null;
        },
        () -> {
          loadDataToF(csvFilePath);
          return null;
        }
    );

    try {
      List<Future<Void>> futures = executor.invokeAll(tasks); // Block until all are done
      for (Future<Void> future : futures) {
        future.get(); // Check for exceptions during execution
      }
    } catch (Exception e) {
      System.out.println("Error during concurrent CSV loading: " + e.getMessage());
    }
  }

  private boolean checkAllCsvFilesExists(String csvFilePath) {
    File folder = new File(csvFilePath);
    File[] csvFiles = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));
    if (csvFiles == null) {
      System.out.println("Invalid directory or no CSV files found.");
      return false;
    }

    List<String> csvFileNames = new ArrayList<>();
    for (File file : csvFiles) {
      String name = file.getName();
      if (name.toLowerCase().endsWith(".csv")) {
        csvFileNames.add(name.substring(0, name.length() - 4)); // Remove ".csv"
      }
    }

    for (String table : tables) {
      if (!csvFileNames.contains(table)) {
        System.out.println("Missing CSV file for table: " + table);
        return false;
      }
    }

    return true;
  }

  private Integer getNullInt() {
    nullInt--;
    return nullInt;
  }

  private Double getNullDouble() {
    nullDouble--;
    return nullDouble;
  }

  private Integer parseNullableInt(String value) {
    if (value == null || value.trim().isEmpty()) {
      return getNullInt();
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return (int) Double.parseDouble(value); // fallback if it's a decimal like "2.0"
    }
  }

  private Double parseNullableDouble(String value) {
    if (value == null || value.trim().isEmpty()) {
      return getNullDouble();
    }
    return Double.parseDouble(value);
  }

  private int parseNonNullableInt(String value) {
    return (int) Double.parseDouble(value);
  }

  private double parseNonNullableDouble(String value) {
    return Double.parseDouble(value);
  }

  public List<Object[]> getA() {
    return A;
  }

  public List<Object[]> getB() {
    return B;
  }

  public List<Object[]> getC() {
    return C;
  }

  public List<Object[]> getD() {
    return D;
  }

  public List<Object[]> getE() {
    return E;
  }

  public List<Object[]> getF() {
    return F;
  }
}
