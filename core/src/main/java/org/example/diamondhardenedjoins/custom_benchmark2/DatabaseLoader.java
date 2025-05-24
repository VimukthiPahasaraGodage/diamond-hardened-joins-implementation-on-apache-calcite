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

package org.example.diamondhardenedjoins.custom_benchmark2;

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
      "countries",
      "regions",
      "categories",
      "subcategories",
      "suppliers",
      "manufacturers",
      "products",
      "reviewers",
      "reviews"
  ));

  private final List<Object[]> countries = new ArrayList<>();
  private final List<Object[]> regions = new ArrayList<>();
  private final List<Object[]> categories = new ArrayList<>();
  private final List<Object[]> subcategories = new ArrayList<>();
  private final List<Object[]> suppliers = new ArrayList<>();
  private final List<Object[]> manufacturers = new ArrayList<>();
  private final List<Object[]> products = new ArrayList<>();
  private final List<Object[]> reviewers = new ArrayList<>();
  private final List<Object[]> reviews = new ArrayList<>();

  private int nullInt = -1;
  private double nullDouble = -1;

  private DatabaseLoader() throws Exception {
    String csvFilePath = "C:\\Benchmark2_Dataset";
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
    System.out.println("countries: " + countries.size());
    System.out.println("regions: " + regions.size());
    System.out.println("categories: " + categories.size());
    System.out.println("subcategories: " + subcategories.size());
    System.out.println("suppliers: " + suppliers.size());
    System.out.println("manufacturers: " + manufacturers.size());
    System.out.println("products: " + products.size());
    System.out.println("reviewers: " + reviewers.size());
    System.out.println("reviews: " + reviews.size());
  }

  private void loadDataToProducts(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\products.csv"))
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
              row[1],
              parseNonNullableInt(row[2]),
              parseNonNullableInt(row[3]),
              parseNonNullableInt(row[4])
          };
          products.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in products.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to products");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to products" + e.getMessage());
    }
  }

  private void loadDataToSubcategories(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\subcategories.csv"))
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
              row[2]
          };
          subcategories.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in subcategories.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to subcategories");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to subcategories" + e.getMessage());
    }
  }

  private void loadDataToCategories(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\categories.csv"))
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
              row[1]
          };
          categories.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in categories.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to categories");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to categories" + e.getMessage());
    }
  }

  private void loadDataToSuppliers(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\suppliers.csv"))
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
              row[2]
          };
          suppliers.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in suppliers.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to suppliers");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to suppliers" + e.getMessage());
    }
  }

  private void loadDataToManufacturers(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\manufacturers.csv"))
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
              row[2]
          };
          manufacturers.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in manufacturers.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to manufacturers");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to manufacturers" + e.getMessage());
    }
  }

  private void loadDataToRegions(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\regions.csv"))
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
              row[2]
          };
          regions.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in regions.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to regions");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to regions" + e.getMessage());
    }
  }

  private void loadDataToCountries(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\countries.csv"))
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
              row[1]
          };
          countries.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in countries.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to countries");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to countries" + e.getMessage());
    }
  }

  private void loadDataToReviews(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\reviews.csv"))
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
          reviews.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in reviews.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to reviews");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to reviews" + e.getMessage());
    }
  }

  private void loadDataToReviewers(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\reviewers.csv"))
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
              row[1],
              parseNonNullableInt(row[2])
          };
          reviewers.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in reviewers.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to reviewers");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to reviewers" + e.getMessage());
    }
  }

  private void loadDataToArrayLists(String csvFilePath) {
    ExecutorService executor = Executors.newFixedThreadPool(21);

    List<Callable<Void>> tasks = Arrays.asList(
        () -> {
          loadDataToCountries(csvFilePath);
          return null;
        },
        () -> {
          loadDataToRegions(csvFilePath);
          return null;
        },
        () -> {
          loadDataToCategories(csvFilePath);
          return null;
        },
        () -> {
          loadDataToSubcategories(csvFilePath);
          return null;
        },
        () -> {
          loadDataToSuppliers(csvFilePath);
          return null;
        },
        () -> {
          loadDataToManufacturers(csvFilePath);
          return null;
        },
        () -> {
          loadDataToProducts(csvFilePath);
          return null;
        },
        () -> {
          loadDataToReviewers(csvFilePath);
          return null;
        },
        () -> {
          loadDataToReviews(csvFilePath);
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

  public List<Object[]> getCountries() {
    return countries;
  }

  public List<Object[]> getRegions() {
    return regions;
  }

  public List<Object[]> getCategories() {
    return categories;
  }

  public List<Object[]> getSubcategories() {
    return subcategories;
  }

  public List<Object[]> getSuppliers() {
    return suppliers;
  }

  public List<Object[]> getManufacturers() {
    return manufacturers;
  }

  public List<Object[]> getProducts() {
    return products;
  }

  public List<Object[]> getReviewers() {
    return reviewers;
  }

  public List<Object[]> getReviews() {
    return reviews;
  }
}
