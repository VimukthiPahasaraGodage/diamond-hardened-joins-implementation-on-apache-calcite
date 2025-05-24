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
      "aka_name",
      "aka_title",
      "cast_info",
      "char_name",
      "comp_cast_type",
      "company_name",
      "company_type",
      "complete_cast",
      "info_type",
      "keyword",
      "kind_type",
      "link_type",
      "movie_companies",
      "movie_info_idx",
      "movie_keyword",
      "movie_link",
      "name",
      "role_type",
      "title",
      "movie_info",
      "person_info"
  ));
  private final List<Object[]> AKA_NAME = new ArrayList<>();
  private final List<Object[]> AKA_TITLE = new ArrayList<>();
  private final List<Object[]> CAST_INFO = new ArrayList<>();
  private final List<Object[]> CHAR_NAME = new ArrayList<>();
  private final List<Object[]> COMP_CAST_TYPE = new ArrayList<>();
  private final List<Object[]> COMPANY_NAME = new ArrayList<>();
  private final List<Object[]> COMPANY_TYPE = new ArrayList<>();
  private final List<Object[]> COMPLETE_CAST = new ArrayList<>();
  private final List<Object[]> INFO_TYPE = new ArrayList<>();
  private final List<Object[]> KEYWORD = new ArrayList<>();
  private final List<Object[]> KIND_TYPE = new ArrayList<>();
  private final List<Object[]> LINK_TYPE = new ArrayList<>();
  private final List<Object[]> MOVIE_COMPANIES = new ArrayList<>();
  private final List<Object[]> MOVIE_INFO_IDX = new ArrayList<>();
  private final List<Object[]> MOVIE_KEYWORD = new ArrayList<>();
  private final List<Object[]> MOVIE_LINK = new ArrayList<>();
  private final List<Object[]> NAME = new ArrayList<>();
  private final List<Object[]> ROLE_TYPE = new ArrayList<>();
  private final List<Object[]> TITLE = new ArrayList<>();
  private final List<Object[]> MOVIE_INFO = new ArrayList<>();
  private final List<Object[]> PERSON_INFO = new ArrayList<>();
  private int nullInt = -1;
  private double nullDouble = -1;

  private DatabaseLoader() throws Exception {
    String csvFilePath = "C:\\JOB_dataset";
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
    System.out.println("AKA_NAME: " + AKA_NAME.size());
    System.out.println("AKA_TITLE: " + AKA_TITLE.size());
    System.out.println("CAST_INFO: " + CAST_INFO.size());
    System.out.println("CHAR_NAME: " + CHAR_NAME.size());
    System.out.println("COMP_CAST_TYPE: " + COMP_CAST_TYPE.size());
    System.out.println("COMPANY_NAME: " + COMPANY_NAME.size());
    System.out.println("COMPANY_TYPE: " + COMPANY_TYPE.size());
    System.out.println("COMPLETE_CAST: " + COMPLETE_CAST.size());
    System.out.println("INFO_TYPE: " + INFO_TYPE.size());
    System.out.println("KEYWORD: " + KEYWORD.size());
    System.out.println("KIND_TYPE: " + KIND_TYPE.size());
    System.out.println("LINK_TYPE: " + LINK_TYPE.size());
    System.out.println("MOVIE_COMPANIES: " + MOVIE_COMPANIES.size());
    System.out.println("MOVIE_INFO_IDX: " + MOVIE_INFO_IDX.size());
    System.out.println("MOVIE_KEYWORD: " + MOVIE_KEYWORD.size());
    System.out.println("MOVIE_LINK: " + MOVIE_LINK.size());
    System.out.println("NAME: " + NAME.size());
    System.out.println("ROLE_TYPE: " + ROLE_TYPE.size());
    System.out.println("TITLE: " + TITLE.size());
    System.out.println("MOVIE_INFO: " + MOVIE_INFO.size());
    System.out.println("PERSON_INFO: " + PERSON_INFO.size());
  }

  private void loadDataToAkaName(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\aka_name.csv"))
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
              row[2],
              row[3],
              row[4],
              row[5],
              row[6],
              row[7]
          };
          AKA_NAME.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in aka_name.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to AKA_NAME");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to AKA_NAME" + e.getMessage());
    }
  }

  private void loadDataToAkaTitle(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\aka_title.csv"))
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
              row[2],
              row[3],
              parseNonNullableInt(row[4]),
              parseNullableInt(row[5]),
              row[6],
              parseNullableInt(row[7]),
              parseNullableInt(row[8]),
              parseNullableInt(row[9]),
              row[10],
              row[11]
          };
          AKA_TITLE.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in aka_title.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to AKA_TITLE");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to AKA_TITLE" + e.getMessage());
    }
  }

  private void loadDataToCastInfo(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\cast_info.csv"))
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
              parseNullableInt(row[3]),
              row[4],
              parseNullableInt(row[5]),
              parseNonNullableInt(row[6])
          };
          CAST_INFO.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in cast_info.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to CAST_INFO");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to CAST_INFO" + e.getMessage());
    }
  }

  private void loadDataToCharName(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\char_name.csv"))
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
              row[2],
              parseNullableInt(row[3]),
              row[4],
              row[5],
              row[6]
          };
          CHAR_NAME.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in char_name.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to CHAR_NAME");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to CHAR_NAME" + e.getMessage());
    }
  }

  private void loadDataToCompCastType(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\comp_cast_type" +
        ".csv"))
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
          COMP_CAST_TYPE.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in comp_cast_type.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to COMP_CAST_TYPE");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to COMP_CAST_TYPE" + e.getMessage());
    }
  }

  private void loadDataToCompanyName(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\company_name.csv"))
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
              row[2],
              parseNullableInt(row[3]),
              row[4],
              row[5],
              row[6]
          };
          COMPANY_NAME.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in company_name.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to COMPANY_NAME");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to COMPANY_NAME" + e.getMessage());
    }
  }

  private void loadDataToCompanyType(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\company_type.csv"))
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
          COMPANY_TYPE.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in company_type.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to COMPANY_TYPE");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to COMPANY_TYPE" + e.getMessage());
    }
  }

  private void loadDataToCompleteCast(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\complete_cast" +
        ".csv"))
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
              parseNonNullableInt(row[3])
          };
          COMPLETE_CAST.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in complete_cast.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to COMPLETE_CAST");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to COMPLETE_CAST" + e.getMessage());
    }
  }

  private void loadDataToInfoType(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\info_type.csv"))
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
          INFO_TYPE.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in info_type.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to INFO_TYPE");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to INFO_TYPE" + e.getMessage());
    }
  }

  private void loadDataToKeyword(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\keyword.csv"))
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
              row[2]
          };
          KEYWORD.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in keyword.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to KEYWORD");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to KEYWORD" + e.getMessage());
    }
  }

  private void loadDataToKindType(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\kind_type.csv"))
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
          KIND_TYPE.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in kind_type.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to KIND_TYPE");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to KIND_TYPE" + e.getMessage());
    }
  }

  private void loadDataToLinkType(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\link_type.csv"))
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
          LINK_TYPE.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in link_type.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to LINK_TYPE");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to LINK_TYPE" + e.getMessage());
    }
  }

  private void loadDataToMovieCompanies(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\movie_companies" +
        ".csv"))
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
              parseNonNullableInt(row[3]),
              row[4]
          };
          MOVIE_COMPANIES.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in movie_companies.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to MOVIE_COMPANIES");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to MOVIE_COMPANIES" + e.getMessage());
    }
  }

  private void loadDataToMovieInfoIdx(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\movie_info_idx" +
        ".csv"))
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
              row[3],
              row[4]
          };
          MOVIE_INFO_IDX.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in movie_info_idx.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to MOVIE_INFO_IDX");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to MOVIE_INFO_IDX" + e.getMessage());
    }
  }

  private void loadDataToMovieKeyword(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\movie_keyword" +
        ".csv"))
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
          MOVIE_KEYWORD.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in movie_keyword.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to MOVIE_KEYWORD");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to MOVIE_KEYWORD" + e.getMessage());
    }
  }

  private void loadDataToMovieLink(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\movie_link.csv"))
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
          MOVIE_LINK.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in movie_link.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to MOVIE_LINK");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to MOVIE_LINK" + e.getMessage());
    }
  }

  private void loadDataToName(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\name.csv"))
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
              row[2],
              parseNullableInt(row[3]),
              row[4],
              row[5],
              row[6],
              row[7],
              row[8]
          };
          NAME.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in name.csv. Line number: " + lineNumber +
              " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to NAME");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to NAME" + e.getMessage());
    }
  }

  private void loadDataToRoleType(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\role_type.csv"))
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
          ROLE_TYPE.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in role_type.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to ROLE_TYPE");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to ROLE_TYPE" + e.getMessage());
    }
  }

  private void loadDataToTitle(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\title.csv"))
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
              row[2],
              parseNonNullableInt(row[3]),
              parseNullableInt(row[4]),
              parseNullableInt(row[5]),
              row[6],
              parseNullableInt(row[7]),
              parseNullableInt(row[8]),
              parseNullableInt(row[9]),
              row[10],
              row[11]
          };
          TITLE.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in title.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to TITLE");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to TITLE" + e.getMessage());
    }
  }

  private void loadDataToMovieInfo(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\movie_info.csv"))
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
              row[3],
              row[4]
          };
          MOVIE_INFO.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in movie_info.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to MOVIE_INFO");
    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to MOVIE_INFO" + e.getMessage());
    }
  }

  private void loadDataToPersonInfo(String csvFilePath) {
    try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvFilePath + "\\person_info.csv"))
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
              row[3],
              row[4]
          };
          PERSON_INFO.add(newRow);
        } catch (Exception inner) {
          totalLinesSkipped++;
          System.err.println("Skipping malformed line in person_info.csv. Line number: " + lineNumber + " Total lines skipped: " + totalLinesSkipped);
        }
      }
      System.out.println("Finished loading data to PERSON_INFO");

    } catch (Exception e) {
      System.out.println("Unexpected error occurred while loading data to PERSON_INFO" + e.getMessage());
    }
  }

  private void loadDataToArrayLists(String csvFilePath) {
    ExecutorService executor = Executors.newFixedThreadPool(21);

    List<Callable<Void>> tasks = Arrays.asList(
        () -> {
          loadDataToAkaName(csvFilePath);
          return null;
        },
        () -> {
          loadDataToAkaTitle(csvFilePath);
          return null;
        },
        () -> {
          loadDataToCastInfo(csvFilePath);
          return null;
        },
        () -> {
          loadDataToCharName(csvFilePath);
          return null;
        },
        () -> {
          loadDataToCompCastType(csvFilePath);
          return null;
        },
        () -> {
          loadDataToCompanyName(csvFilePath);
          return null;
        },
        () -> {
          loadDataToCompanyType(csvFilePath);
          return null;
        },
        () -> {
          loadDataToCompleteCast(csvFilePath);
          return null;
        },
        () -> {
          loadDataToInfoType(csvFilePath);
          return null;
        },
        () -> {
          loadDataToKeyword(csvFilePath);
          return null;
        },
        () -> {
          loadDataToKindType(csvFilePath);
          return null;
        },
        () -> {
          loadDataToLinkType(csvFilePath);
          return null;
        },
        () -> {
          loadDataToMovieCompanies(csvFilePath);
          return null;
        },
        () -> {
          loadDataToMovieInfoIdx(csvFilePath);
          return null;
        },
        () -> {
          loadDataToMovieKeyword(csvFilePath);
          return null;
        },
        () -> {
          loadDataToMovieLink(csvFilePath);
          return null;
        },
        () -> {
          loadDataToName(csvFilePath);
          return null;
        },
        () -> {
          loadDataToRoleType(csvFilePath);
          return null;
        },
        () -> {
          loadDataToTitle(csvFilePath);
          return null;
        },
        () -> {
          loadDataToMovieInfo(csvFilePath);
          return null;
        },
        () -> {
          loadDataToPersonInfo(csvFilePath);
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

  public List<Object[]> getAKA_NAME() {
    return AKA_NAME;
  }

  public List<Object[]> getAKA_TITLE() {
    return AKA_TITLE;
  }

  public List<Object[]> getCAST_INFO() {
    return CAST_INFO;
  }

  public List<Object[]> getCHAR_NAME() {
    return CHAR_NAME;
  }

  public List<Object[]> getCOMP_CAST_TYPE() {
    return COMP_CAST_TYPE;
  }

  public List<Object[]> getCOMPANY_NAME() {
    return COMPANY_NAME;
  }

  public List<Object[]> getCOMPANY_TYPE() {
    return COMPANY_TYPE;
  }

  public List<Object[]> getCOMPLETE_CAST() {
    return COMPLETE_CAST;
  }

  public List<Object[]> getINFO_TYPE() {
    return INFO_TYPE;
  }

  public List<Object[]> getKEYWORD() {
    return KEYWORD;
  }

  public List<Object[]> getKIND_TYPE() {
    return KIND_TYPE;
  }

  public List<Object[]> getLINK_TYPE() {
    return LINK_TYPE;
  }

  public List<Object[]> getMOVIE_COMPANIES() {
    return MOVIE_COMPANIES;
  }

  public List<Object[]> getMOVIE_INFO_IDX() {
    return MOVIE_INFO_IDX;
  }

  public List<Object[]> getMOVIE_KEYWORD() {
    return MOVIE_KEYWORD;
  }

  public List<Object[]> getMOVIE_LINK() {
    return MOVIE_LINK;
  }

  public List<Object[]> getNAME() {
    return NAME;
  }

  public List<Object[]> getROLE_TYPE() {
    return ROLE_TYPE;
  }

  public List<Object[]> getTITLE() {
    return TITLE;
  }

  public List<Object[]> getMOVIE_INFO() {
    return MOVIE_INFO;
  }

  public List<Object[]> getPERSON_INFO() {
    return PERSON_INFO;
  }
}
