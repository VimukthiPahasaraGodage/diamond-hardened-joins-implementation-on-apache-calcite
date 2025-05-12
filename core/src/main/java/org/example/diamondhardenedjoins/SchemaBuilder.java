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

public class SchemaBuilder {
  private static SchemaBuilder schemaBuilder;
  private RelDataTypeFactory typeFactory;
  private CalciteSchema schema;

  private SchemaBuilder() throws Exception {
    System.out.println("Loading the CSV files into ArrayLists...");
    DatabaseLoader databaseLoader = DatabaseLoader.getInstance();
    System.out.println("Building the database schema...");
    buildSchema(databaseLoader);
  }

  public static SchemaBuilder getInstance() throws Exception {
    if (schemaBuilder == null) {
      schemaBuilder = new SchemaBuilder();
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

  private void buildSchema(DatabaseLoader databaseLoader) {
    typeFactory = new JavaTypeFactoryImpl();
    schema = CalciteSchema.createRootSchema(true);

    RelDataTypeFactory.Builder aka_nameType = new RelDataTypeFactory.Builder(typeFactory);
    aka_nameType.add("id", SqlTypeName.INTEGER);
    aka_nameType.add("person_id", SqlTypeName.INTEGER);
    aka_nameType.add("name", SqlTypeName.VARCHAR);
    aka_nameType.add("imdb_index", SqlTypeName.VARCHAR);
    aka_nameType.add("name_pcode_cf", SqlTypeName.VARCHAR);
    aka_nameType.add("name_pcode_nf", SqlTypeName.VARCHAR);
    aka_nameType.add("surname_pcode", SqlTypeName.VARCHAR);
    aka_nameType.add("md5sum", SqlTypeName.VARCHAR);

    ListTable aka_nameTable = new ListTable(aka_nameType.build(), databaseLoader.getAKA_NAME());
    schema.add("aka_name", aka_nameTable);

    // =============================================================================================

    RelDataTypeFactory.Builder aka_titleType = new RelDataTypeFactory.Builder(typeFactory);
    aka_titleType.add("id", SqlTypeName.INTEGER);
    aka_titleType.add("movie_id", SqlTypeName.INTEGER);
    aka_titleType.add("title", SqlTypeName.VARCHAR);
    aka_titleType.add("imdb_index", SqlTypeName.VARCHAR);
    aka_titleType.add("kind_id", SqlTypeName.INTEGER);
    aka_titleType.add("production_year", SqlTypeName.INTEGER);
    aka_titleType.add("phonetic_code", SqlTypeName.VARCHAR);
    aka_titleType.add("episode_of_id", SqlTypeName.INTEGER);
    aka_titleType.add("season_nr", SqlTypeName.INTEGER);
    aka_titleType.add("episode_nr", SqlTypeName.INTEGER);
    aka_titleType.add("note", SqlTypeName.VARCHAR);
    aka_titleType.add("md5sum", SqlTypeName.VARCHAR);

    ListTable aka_titleTable = new ListTable(aka_titleType.build(), databaseLoader.getAKA_TITLE());
    schema.add("aka_title", aka_titleTable);

    // =============================================================================================

    RelDataTypeFactory.Builder cast_infoType = new RelDataTypeFactory.Builder(typeFactory);
    cast_infoType.add("id", SqlTypeName.INTEGER);
    cast_infoType.add("person_id", SqlTypeName.INTEGER);
    cast_infoType.add("movie_id", SqlTypeName.INTEGER);
    cast_infoType.add("person_role_id", SqlTypeName.INTEGER);
    cast_infoType.add("note", SqlTypeName.VARCHAR);
    cast_infoType.add("nr_order", SqlTypeName.INTEGER);
    cast_infoType.add("role_id", SqlTypeName.INTEGER);

    ListTable cast_infoTable = new ListTable(cast_infoType.build(), databaseLoader.getCAST_INFO());
    schema.add("cast_info", cast_infoTable);

    // =============================================================================================

    RelDataTypeFactory.Builder char_nameType = new RelDataTypeFactory.Builder(typeFactory);
    char_nameType.add("id", SqlTypeName.INTEGER);
    char_nameType.add("name", SqlTypeName.VARCHAR);
    char_nameType.add("imdb_index", SqlTypeName.VARCHAR);
    char_nameType.add("imdb_id", SqlTypeName.INTEGER);
    char_nameType.add("name_pcode_nf", SqlTypeName.VARCHAR);
    char_nameType.add("surname_pcode", SqlTypeName.VARCHAR);
    char_nameType.add("md5sum", SqlTypeName.VARCHAR);

    ListTable char_nameTable = new ListTable(char_nameType.build(), databaseLoader.getCHAR_NAME());
    schema.add("char_name", char_nameTable);

    // =============================================================================================

    RelDataTypeFactory.Builder comp_cast_typeType = new RelDataTypeFactory.Builder(typeFactory);
    comp_cast_typeType.add("id", SqlTypeName.INTEGER);
    comp_cast_typeType.add("kind", SqlTypeName.VARCHAR);

    ListTable comp_cast_typeTable = new ListTable(comp_cast_typeType.build(),
        databaseLoader.getCOMP_CAST_TYPE());
    schema.add("comp_cast_type", comp_cast_typeTable);

    // =============================================================================================

    RelDataTypeFactory.Builder company_nameType = new RelDataTypeFactory.Builder(typeFactory);
    company_nameType.add("id", SqlTypeName.INTEGER);
    company_nameType.add("name", SqlTypeName.VARCHAR);
    company_nameType.add("country_code", SqlTypeName.VARCHAR);
    company_nameType.add("imdb_id", SqlTypeName.INTEGER);
    company_nameType.add("name_pcode_nf", SqlTypeName.VARCHAR);
    company_nameType.add("name_pcode_sf", SqlTypeName.VARCHAR);
    company_nameType.add("md5sum", SqlTypeName.VARCHAR);

    ListTable company_nameTable = new ListTable(company_nameType.build(),
        databaseLoader.getCOMPANY_NAME());
    schema.add("company_name", company_nameTable);

    // =============================================================================================

    RelDataTypeFactory.Builder company_typeType = new RelDataTypeFactory.Builder(typeFactory);
    company_typeType.add("id", SqlTypeName.INTEGER);
    company_typeType.add("kind", SqlTypeName.VARCHAR);

    ListTable company_typeTable = new ListTable(company_typeType.build(),
        databaseLoader.getCOMPANY_TYPE());
    schema.add("company_type", company_typeTable);

    // =============================================================================================

    RelDataTypeFactory.Builder complete_castType = new RelDataTypeFactory.Builder(typeFactory);
    complete_castType.add("id", SqlTypeName.INTEGER);
    complete_castType.add("movie_id", SqlTypeName.INTEGER);
    complete_castType.add("subject_id", SqlTypeName.INTEGER);
    complete_castType.add("status_id", SqlTypeName.INTEGER);

    ListTable complete_castTable = new ListTable(complete_castType.build(),
        databaseLoader.getCOMPLETE_CAST());
    schema.add("complete_cast", complete_castTable);

    // =============================================================================================

    RelDataTypeFactory.Builder info_typeType = new RelDataTypeFactory.Builder(typeFactory);
    info_typeType.add("id", SqlTypeName.INTEGER);
    info_typeType.add("info", SqlTypeName.VARCHAR);

    ListTable info_typeTable = new ListTable(info_typeType.build(), databaseLoader.getINFO_TYPE());
    schema.add("info_type", info_typeTable);

    // =============================================================================================

    RelDataTypeFactory.Builder keywordType = new RelDataTypeFactory.Builder(typeFactory);
    keywordType.add("id", SqlTypeName.INTEGER);
    keywordType.add("keyword", SqlTypeName.VARCHAR);
    keywordType.add("phonetic_code", SqlTypeName.VARCHAR);

    ListTable keywordTable = new ListTable(keywordType.build(), databaseLoader.getKEYWORD());
    schema.add("keyword", keywordTable);

    // =============================================================================================

    RelDataTypeFactory.Builder kind_typeType = new RelDataTypeFactory.Builder(typeFactory);
    kind_typeType.add("id", SqlTypeName.INTEGER);
    kind_typeType.add("kind", SqlTypeName.VARCHAR);

    ListTable kind_typeTable = new ListTable(kind_typeType.build(), databaseLoader.getKIND_TYPE());
    schema.add("kind_type", kind_typeTable);

    // =============================================================================================

    RelDataTypeFactory.Builder link_typeType = new RelDataTypeFactory.Builder(typeFactory);
    link_typeType.add("id", SqlTypeName.INTEGER);
    link_typeType.add("link", SqlTypeName.VARCHAR);

    ListTable link_typeTable = new ListTable(link_typeType.build(), databaseLoader.getLINK_TYPE());
    schema.add("link_type", link_typeTable);

    // =============================================================================================

    RelDataTypeFactory.Builder movie_companiesType = new RelDataTypeFactory.Builder(typeFactory);
    movie_companiesType.add("id", SqlTypeName.INTEGER);
    movie_companiesType.add("movie_id", SqlTypeName.INTEGER);
    movie_companiesType.add("company_id", SqlTypeName.INTEGER);
    movie_companiesType.add("company_type_id", SqlTypeName.INTEGER);
    movie_companiesType.add("note", SqlTypeName.VARCHAR);

    ListTable movie_companiesTable = new ListTable(movie_companiesType.build(),
        databaseLoader.getMOVIE_COMPANIES());
    schema.add("movie_companies", movie_companiesTable);

    // =============================================================================================

    RelDataTypeFactory.Builder movie_info_idxType = new RelDataTypeFactory.Builder(typeFactory);
    movie_info_idxType.add("id", SqlTypeName.INTEGER);
    movie_info_idxType.add("movie_id", SqlTypeName.INTEGER);
    movie_info_idxType.add("info_type_id", SqlTypeName.INTEGER);
    movie_info_idxType.add("info", SqlTypeName.VARCHAR);
    movie_info_idxType.add("note", SqlTypeName.VARCHAR);

    ListTable movie_info_idxTable = new ListTable(movie_info_idxType.build(),
        databaseLoader.getMOVIE_INFO_IDX());
    schema.add("movie_info_idx", movie_info_idxTable);

    // =============================================================================================

    RelDataTypeFactory.Builder movie_keywordType = new RelDataTypeFactory.Builder(typeFactory);
    movie_keywordType.add("id", SqlTypeName.INTEGER);
    movie_keywordType.add("movie_id", SqlTypeName.INTEGER);
    movie_keywordType.add("keyword_id", SqlTypeName.INTEGER);

    ListTable movie_keywordTable = new ListTable(movie_keywordType.build(),
        databaseLoader.getMOVIE_KEYWORD());
    schema.add("movie_keyword", movie_keywordTable);

    // =============================================================================================

    RelDataTypeFactory.Builder movie_linkType = new RelDataTypeFactory.Builder(typeFactory);
    movie_linkType.add("id", SqlTypeName.INTEGER);
    movie_linkType.add("movie_id", SqlTypeName.INTEGER);
    movie_linkType.add("linked_movie_id", SqlTypeName.INTEGER);
    movie_linkType.add("link_type_id", SqlTypeName.INTEGER);

    ListTable movie_linkTable = new ListTable(movie_linkType.build(),
        databaseLoader.getMOVIE_LINK());
    schema.add("movie_link", movie_linkTable);

    // =============================================================================================

    RelDataTypeFactory.Builder nameType = new RelDataTypeFactory.Builder(typeFactory);
    nameType.add("id", SqlTypeName.INTEGER);
    nameType.add("name", SqlTypeName.VARCHAR);
    nameType.add("imdb_index", SqlTypeName.VARCHAR);
    nameType.add("imdb_id", SqlTypeName.INTEGER);
    nameType.add("gender", SqlTypeName.VARCHAR);
    nameType.add("name_pcode_cf", SqlTypeName.VARCHAR);
    nameType.add("name_pcode_nf", SqlTypeName.VARCHAR);
    nameType.add("surname_pcode", SqlTypeName.VARCHAR);
    nameType.add("md5sum", SqlTypeName.VARCHAR);

    ListTable nameTable = new ListTable(nameType.build(), databaseLoader.getNAME());
    schema.add("name", nameTable);

    // =============================================================================================

    RelDataTypeFactory.Builder role_typeType = new RelDataTypeFactory.Builder(typeFactory);
    role_typeType.add("id", SqlTypeName.INTEGER);
    role_typeType.add("role", SqlTypeName.VARCHAR);

    ListTable role_typeTable = new ListTable(role_typeType.build(), databaseLoader.getROLE_TYPE());
    schema.add("role_type", role_typeTable);

    // =============================================================================================

    RelDataTypeFactory.Builder titleType = new RelDataTypeFactory.Builder(typeFactory);
    titleType.add("id", SqlTypeName.INTEGER);
    titleType.add("title", SqlTypeName.VARCHAR);
    titleType.add("imdb_index", SqlTypeName.VARCHAR);
    titleType.add("kind_id", SqlTypeName.INTEGER);
    titleType.add("production_year", SqlTypeName.INTEGER);
    titleType.add("imdb_id", SqlTypeName.INTEGER);
    titleType.add("phonetic_code", SqlTypeName.VARCHAR);
    titleType.add("episode_of_id", SqlTypeName.INTEGER);
    titleType.add("season_nr", SqlTypeName.INTEGER);
    titleType.add("episode_nr", SqlTypeName.INTEGER);
    titleType.add("series_years", SqlTypeName.VARCHAR);
    titleType.add("md5sum", SqlTypeName.VARCHAR);

    ListTable titleTable = new ListTable(titleType.build(), databaseLoader.getTITLE());
    schema.add("title", titleTable);

    // =============================================================================================

    RelDataTypeFactory.Builder movie_infoType = new RelDataTypeFactory.Builder(typeFactory);
    movie_infoType.add("id", SqlTypeName.INTEGER);
    movie_infoType.add("movie_id", SqlTypeName.INTEGER);
    movie_infoType.add("info_type_id", SqlTypeName.INTEGER);
    movie_infoType.add("info", SqlTypeName.VARCHAR);
    movie_infoType.add("note", SqlTypeName.VARCHAR);

    ListTable movie_infoTable = new ListTable(movie_infoType.build(),
        databaseLoader.getMOVIE_INFO());
    schema.add("movie_info", movie_infoTable);

    // =============================================================================================

    RelDataTypeFactory.Builder person_infoType = new RelDataTypeFactory.Builder(typeFactory);
    person_infoType.add("id", SqlTypeName.INTEGER);
    person_infoType.add("person_id", SqlTypeName.INTEGER);
    person_infoType.add("info_type_id", SqlTypeName.INTEGER);
    person_infoType.add("info", SqlTypeName.VARCHAR);
    person_infoType.add("note", SqlTypeName.VARCHAR);

    ListTable person_infoTable = new ListTable(person_infoType.build(),
        databaseLoader.getPERSON_INFO());
    schema.add("person_info", person_infoTable);
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
