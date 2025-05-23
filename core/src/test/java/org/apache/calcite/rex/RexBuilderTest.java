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
package org.apache.calcite.rex;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CustomTypeSystems;
import org.apache.calcite.test.RexImplicationCheckerFixtures;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimeWithTimeZoneString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.TimestampWithTimeZoneString;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.of;

/**
 * Test for {@link RexBuilder}.
 */
class RexBuilderTest {

  private static final int PRECISION = 256;

  /**
   * MySqlTypeFactoryImpl provides a specific implementation of
   * {@link SqlTypeFactoryImpl} which sets precision to 256 for VARCHAR.
   */
  private static class MySqlTypeFactoryImpl extends SqlTypeFactoryImpl {

    MySqlTypeFactoryImpl(RelDataTypeSystem typeSystem) {
      super(typeSystem);
    }

    @Override public RelDataType createTypeWithNullability(
        final RelDataType type,
        final boolean nullable) {
      if (type.getSqlTypeName() == SqlTypeName.VARCHAR) {
        return new BasicSqlType(this.typeSystem, type.getSqlTypeName(),
            PRECISION);
      }
      return super.createTypeWithNullability(type, nullable);
    }
  }


  /**
   * Test RexBuilder.ensureType()
   */
  @Test void testEnsureTypeWithAny() {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder builder = new RexBuilder(typeFactory);

    RexNode node =
        new RexLiteral(Boolean.TRUE,
            typeFactory.createSqlType(SqlTypeName.BOOLEAN), SqlTypeName.BOOLEAN);
    RexNode ensuredNode =
        builder.ensureType(typeFactory.createSqlType(SqlTypeName.ANY), node,
            true);

    assertThat(ensuredNode, is(node));
  }

  /**
   * Test RexBuilder.ensureType()
   */
  @Test void testEnsureTypeWithItself() {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder builder = new RexBuilder(typeFactory);

    RexNode node =
        new RexLiteral(Boolean.TRUE,
            typeFactory.createSqlType(SqlTypeName.BOOLEAN), SqlTypeName.BOOLEAN);
    RexNode ensuredNode =
        builder.ensureType(typeFactory.createSqlType(SqlTypeName.BOOLEAN), node,
            true);

    assertThat(ensuredNode, is(node));
  }

  /**
   * Test RexBuilder.ensureType()
   */
  @Test void testEnsureTypeWithDifference() {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder builder = new RexBuilder(typeFactory);

    RexNode node =
        new RexLiteral(Boolean.TRUE,
            typeFactory.createSqlType(SqlTypeName.BOOLEAN), SqlTypeName.BOOLEAN);
    RexNode ensuredNode =
        builder.ensureType(typeFactory.createSqlType(SqlTypeName.INTEGER), node,
            true);

    assertNotEquals(node, ensuredNode);
    assertThat(typeFactory.createSqlType(SqlTypeName.INTEGER),
        is(ensuredNode.getType()));
  }

  private static final long MOON = -14159025000L;

  private static final int MOON_DAY = -164;

  private static final int MOON_TIME = 10575000;

  /** Tests {@link RexBuilder#makeTimestampLiteral(TimestampString, int)}. */
  @Test void testTimestampLiteral() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType timestampType =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    final RelDataType timestampType3 =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3);
    final RelDataType timestampType9 =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 9);
    final RelDataType timestampType18 =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 18);
    final RexBuilder builder = new RexBuilder(typeFactory);

    // Old way: provide a Calendar
    final Calendar calendar = Util.calendar();
    calendar.set(1969, Calendar.JULY, 21, 2, 56, 15); // one small step
    calendar.set(Calendar.MILLISECOND, 0);
    checkTimestamp(builder.makeLiteral(calendar, timestampType));

    // Old way #2: Provide a Long
    checkTimestamp(builder.makeLiteral(MOON, timestampType));

    // The new way
    final TimestampString ts = new TimestampString(1969, 7, 21, 2, 56, 15);
    checkTimestamp(builder.makeLiteral(ts, timestampType));

    // Now with milliseconds
    final TimestampString ts2 = ts.withMillis(56);
    assertThat(ts2, hasToString("1969-07-21 02:56:15.056"));
    final RexLiteral literal2 = builder.makeLiteral(ts2, timestampType3);
    assertThat(literal2.getValueAs(TimestampString.class),
        hasToString("1969-07-21 02:56:15.056"));

    // Now with nanoseconds
    final TimestampString ts3 = ts.withNanos(56);
    final RexLiteral literal3 = builder.makeLiteral(ts3, timestampType9);
    assertThat(literal3.getValueAs(TimestampString.class),
        hasToString("1969-07-21 02:56:15"));
    final TimestampString ts3b = ts.withNanos(2345678);
    final RexLiteral literal3b = builder.makeLiteral(ts3b, timestampType9);
    assertThat(literal3b.getValueAs(TimestampString.class),
        hasToString("1969-07-21 02:56:15.002"));

    // Now with a very long fraction
    final TimestampString ts4 = ts.withFraction("102030405060708090102");
    final RexLiteral literal4 = builder.makeLiteral(ts4, timestampType18);
    assertThat(literal4.getValueAs(TimestampString.class),
        hasToString("1969-07-21 02:56:15.102"));
  }

  /** Test cases for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6389">[CALCITE-6389]
   * RexBuilder.removeCastFromLiteral does not preserve semantics for some types of literal</a>. */
  @Test void testRemoveCast() {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder builder = new RexBuilder(typeFactory);

    // Can remove cast of an integer to an integer
    BigDecimal value = new BigDecimal(10);
    RelDataType toType = builder.typeFactory.createSqlType(SqlTypeName.INTEGER);
    assertTrue(builder.canRemoveCastFromLiteral(toType, value, SqlTypeName.INTEGER));

    // Can remove cast from integer to decimal
    toType = builder.typeFactory.createSqlType(SqlTypeName.DECIMAL);
    assertTrue(builder.canRemoveCastFromLiteral(toType, value, SqlTypeName.INTEGER));

    // 250 is too large for a TINYINT
    value = new BigDecimal(250);
    toType = builder.typeFactory.createSqlType(SqlTypeName.TINYINT);
    assertFalse(builder.canRemoveCastFromLiteral(toType, value, SqlTypeName.INTEGER));

    // 50 isn't too large for a TINYINT
    value = new BigDecimal(50);
    toType = builder.typeFactory.createSqlType(SqlTypeName.TINYINT);
    assertTrue(builder.canRemoveCastFromLiteral(toType, value, SqlTypeName.INTEGER));

    // 120.25 cannot be represented with precision 2 and scale 2 without loss
    value = new BigDecimal("120.25");
    toType = builder.typeFactory.createSqlType(SqlTypeName.DECIMAL, 2, 2);
    assertFalse(builder.canRemoveCastFromLiteral(toType, value, SqlTypeName.DECIMAL));

    // 120.25 cannot be represented with precision 5 and scale 1 without rounding
    value = new BigDecimal("120.25");
    toType = builder.typeFactory.createSqlType(SqlTypeName.DECIMAL, 5, 1);
    assertFalse(builder.canRemoveCastFromLiteral(toType, value, SqlTypeName.DECIMAL));

    // longmax + 1 cannot be represented as a long
    value = new BigDecimal(Long.MAX_VALUE).add(BigDecimal.ONE);
    toType = builder.typeFactory.createSqlType(SqlTypeName.BIGINT);
    assertFalse(builder.canRemoveCastFromLiteral(toType, value, SqlTypeName.DECIMAL));

    // Cast to decimal of an INTERVAL '5' seconds cannot be removed
    value = new BigDecimal("5");
    toType = builder.typeFactory.createSqlType(SqlTypeName.DECIMAL, 5, 1);
    assertFalse(builder.canRemoveCastFromLiteral(toType, value, SqlTypeName.INTERVAL_SECOND));

    // Cast to decimal of an INTERVAL '5' minutes cannot be removed
    value = new BigDecimal("5");
    toType = builder.typeFactory.createSqlType(SqlTypeName.DECIMAL, 5, 1);
    assertFalse(builder.canRemoveCastFromLiteral(toType, value, SqlTypeName.INTERVAL_MINUTE));
  }

  @Test void testTimestampString() {
    final TimestampString ts = new TimestampString(1969, 7, 21, 2, 56, 15);
    assertThat(ts, hasToString("1969-07-21 02:56:15"));
    assertThat(ts.round(1), is(ts));

    // Now with milliseconds
    final TimestampString ts2 = ts.withMillis(56);
    assertThat(ts2, hasToString("1969-07-21 02:56:15.056"));

    // toString
    assertThat(ts2.round(1), hasToString("1969-07-21 02:56:15"));
    assertThat(ts2.round(2), hasToString("1969-07-21 02:56:15.05"));
    assertThat(ts2.round(3), hasToString("1969-07-21 02:56:15.056"));
    assertThat(ts2.round(4), hasToString("1969-07-21 02:56:15.056"));

    assertThat(ts2.toString(6), is("1969-07-21 02:56:15.056000"));
    assertThat(ts2.toString(1), is("1969-07-21 02:56:15.0"));
    assertThat(ts2.toString(0), is("1969-07-21 02:56:15"));

    assertThat(ts2.round(0), hasToString("1969-07-21 02:56:15"));
    assertThat(ts2.round(0).toString(0), is("1969-07-21 02:56:15"));
    assertThat(ts2.round(0).toString(1), is("1969-07-21 02:56:15.0"));
    assertThat(ts2.round(0).toString(2), is("1969-07-21 02:56:15.00"));

    // Now with milliseconds ending in zero (3 equivalent strings).
    final TimestampString ts3 = ts.withMillis(10);
    assertThat(ts3, hasToString("1969-07-21 02:56:15.01"));

    final TimestampString ts3b = new TimestampString("1969-07-21 02:56:15.01");
    assertThat(ts3b, hasToString("1969-07-21 02:56:15.01"));
    assertThat(ts3b, is(ts3));

    final TimestampString ts3c = new TimestampString("1969-07-21 02:56:15.010");
    assertThat(ts3c, hasToString("1969-07-21 02:56:15.01"));
    assertThat(ts3c, is(ts3));

    // Now with nanoseconds
    final TimestampString ts4 = ts.withNanos(56);
    assertThat(ts4, hasToString("1969-07-21 02:56:15.000000056"));

    // Check rounding; uses RoundingMode.DOWN
    final TimestampString ts5 = ts.withNanos(2345670);
    assertThat(ts5, hasToString("1969-07-21 02:56:15.00234567"));
    assertThat(ts5.round(0), hasToString("1969-07-21 02:56:15"));
    assertThat(ts5.round(1), hasToString("1969-07-21 02:56:15"));
    assertThat(ts5.round(2), hasToString("1969-07-21 02:56:15"));
    assertThat(ts5.round(3), hasToString("1969-07-21 02:56:15.002"));
    assertThat(ts5.round(4), hasToString("1969-07-21 02:56:15.0023"));
    assertThat(ts5.round(5), hasToString("1969-07-21 02:56:15.00234"));
    assertThat(ts5.round(6), hasToString("1969-07-21 02:56:15.002345"));
    assertThat(ts5.round(600), hasToString("1969-07-21 02:56:15.00234567"));

    // Now with a very long fraction
    final TimestampString ts6 = ts.withFraction("102030405060708090102");
    assertThat(ts6, hasToString("1969-07-21 02:56:15.102030405060708090102"));

    // From milliseconds
    final TimestampString ts7 =
        TimestampString.fromMillisSinceEpoch(1456513560123L);
    assertThat(ts7, hasToString("2016-02-26 19:06:00.123"));

    final TimestampString ts8 =
        TimestampString.fromMillisSinceEpoch(1456513560120L);
    assertThat(ts8, hasToString("2016-02-26 19:06:00.12"));

    final TimestampString ts9 = ts8.withFraction("9876543210");
    assertThat(ts9, hasToString("2016-02-26 19:06:00.987654321"));

    // TimestampString.toCalendar
    final Calendar c = ts9.toCalendar();
    assertThat(c.get(Calendar.ERA), is(1)); // CE
    assertThat(c.get(Calendar.YEAR), is(2016));
    assertThat(c.get(Calendar.MONTH), is(1)); // February
    assertThat(c.get(Calendar.DATE), is(26));
    assertThat(c.get(Calendar.HOUR_OF_DAY), is(19));
    assertThat(c.get(Calendar.MINUTE), is(6));
    assertThat(c.get(Calendar.SECOND), is(0));
    assertThat(c.get(Calendar.MILLISECOND), is(987)); // RoundingMode.DOWN
    assertThat(ts9.getMillisSinceEpoch(), is(c.getTimeInMillis()));

    // TimestampString.fromCalendarFields
    c.set(Calendar.YEAR, 1969);
    final TimestampString ts10 = TimestampString.fromCalendarFields(c);
    assertThat(ts10, hasToString("1969-02-26 19:06:00.987"));
    assertThat(ts10.getMillisSinceEpoch(), is(c.getTimeInMillis()));
  }

  @Test void testTimeString() {
    final TimeString t = new TimeString(2, 56, 15);
    assertThat(t, hasToString("02:56:15"));
    assertThat(t.round(1), is(t));

    // Now with milliseconds
    final TimeString t2 = t.withMillis(56);
    assertThat(t2, hasToString("02:56:15.056"));

    // toString
    assertThat(t2.round(1), hasToString("02:56:15"));
    assertThat(t2.round(2), hasToString("02:56:15.05"));
    assertThat(t2.round(3), hasToString("02:56:15.056"));
    assertThat(t2.round(4), hasToString("02:56:15.056"));

    assertThat(t2.toString(6), is("02:56:15.056000"));
    assertThat(t2.toString(1), is("02:56:15.0"));
    assertThat(t2.toString(0), is("02:56:15"));

    assertThat(t2.round(0), hasToString("02:56:15"));
    assertThat(t2.round(0).toString(0), is("02:56:15"));
    assertThat(t2.round(0).toString(1), is("02:56:15.0"));
    assertThat(t2.round(0).toString(2), is("02:56:15.00"));

    // Now with milliseconds ending in zero (3 equivalent strings).
    final TimeString t3 = t.withMillis(10);
    assertThat(t3, hasToString("02:56:15.01"));

    final TimeString t3b = new TimeString("02:56:15.01");
    assertThat(t3b, hasToString("02:56:15.01"));
    assertThat(t3b, is(t3));

    final TimeString t3c = new TimeString("02:56:15.010");
    assertThat(t3c, hasToString("02:56:15.01"));
    assertThat(t3c, is(t3));

    // Now with nanoseconds
    final TimeString t4 = t.withNanos(56);
    assertThat(t4, hasToString("02:56:15.000000056"));

    // Check rounding; uses RoundingMode.DOWN
    final TimeString t5 = t.withNanos(2345670);
    assertThat(t5, hasToString("02:56:15.00234567"));
    assertThat(t5.round(0), hasToString("02:56:15"));
    assertThat(t5.round(1), hasToString("02:56:15"));
    assertThat(t5.round(2), hasToString("02:56:15"));
    assertThat(t5.round(3), hasToString("02:56:15.002"));
    assertThat(t5.round(4), hasToString("02:56:15.0023"));
    assertThat(t5.round(5), hasToString("02:56:15.00234"));
    assertThat(t5.round(6), hasToString("02:56:15.002345"));
    assertThat(t5.round(600), hasToString("02:56:15.00234567"));

    // Now with a very long fraction
    final TimeString t6 = t.withFraction("102030405060708090102");
    assertThat(t6, hasToString("02:56:15.102030405060708090102"));
  }

  private void checkTimestamp(RexLiteral literal) {
    assertThat(literal, hasToString("1969-07-21 02:56:15"));
    assertThat(literal.getValue() instanceof Calendar, is(true));
    assertThat(literal.getValue2() instanceof Long, is(true));
    assertThat(literal.getValue3() instanceof Long, is(true));
    assertThat((Long) literal.getValue2(), is(MOON));
    assertThat(literal.getValueAs(Calendar.class), notNullValue());
    assertThat(literal.getValueAs(TimestampString.class), notNullValue());
  }

  /** Tests
   * {@link RexBuilder#makeTimestampWithLocalTimeZoneLiteral(TimestampString, int)}. */
  @Test void testTimestampWithLocalTimeZoneLiteral() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType timestampType =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    final RelDataType timestampType3 =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3);
    final RelDataType timestampType9 =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 9);
    final RelDataType timestampType18 =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 18);
    final RexBuilder builder = new RexBuilder(typeFactory);

    // The new way
    final TimestampWithTimeZoneString ts =
        new TimestampWithTimeZoneString(1969, 7, 21, 2, 56, 15,
            TimeZone.getTimeZone("PST").getID());
    checkTimestampWithLocalTimeZone(
        builder.makeLiteral(ts.getLocalTimestampString(), timestampType));

    // Now with milliseconds
    final TimestampWithTimeZoneString ts2 = ts.withMillis(56);
    assertThat(ts2, hasToString("1969-07-21 02:56:15.056 PST"));
    final RexLiteral literal2 =
        builder.makeLiteral(ts2.getLocalTimestampString(), timestampType3);
    assertThat(literal2.getValue(), hasToString("1969-07-21 02:56:15.056"));

    // Now with nanoseconds
    final TimestampWithTimeZoneString ts3 = ts.withNanos(56);
    final RexLiteral literal3 =
        builder.makeLiteral(ts3.getLocalTimestampString(), timestampType9);
    assertThat(literal3.getValueAs(TimestampString.class),
        hasToString("1969-07-21 02:56:15"));
    final TimestampWithTimeZoneString ts3b = ts.withNanos(2345678);
    final RexLiteral literal3b =
        builder.makeLiteral(ts3b.getLocalTimestampString(), timestampType9);
    assertThat(literal3b.getValueAs(TimestampString.class),
        hasToString("1969-07-21 02:56:15.002"));

    // Now with a very long fraction
    final TimestampWithTimeZoneString ts4 = ts.withFraction("102030405060708090102");
    final RexLiteral literal4 =
        builder.makeLiteral(ts4.getLocalTimestampString(), timestampType18);
    assertThat(literal4.getValueAs(TimestampString.class),
        hasToString("1969-07-21 02:56:15.102"));

    // toString
    assertThat(ts2.round(1), hasToString("1969-07-21 02:56:15 PST"));
    assertThat(ts2.round(2), hasToString("1969-07-21 02:56:15.05 PST"));
    assertThat(ts2.round(3), hasToString("1969-07-21 02:56:15.056 PST"));
    assertThat(ts2.round(4), hasToString("1969-07-21 02:56:15.056 PST"));

    assertThat(ts2.toString(6), is("1969-07-21 02:56:15.056000 PST"));
    assertThat(ts2.toString(1), is("1969-07-21 02:56:15.0 PST"));
    assertThat(ts2.toString(0), is("1969-07-21 02:56:15 PST"));

    assertThat(ts2.round(0), hasToString("1969-07-21 02:56:15 PST"));
    assertThat(ts2.round(0).toString(0), is("1969-07-21 02:56:15 PST"));
    assertThat(ts2.round(0).toString(1), is("1969-07-21 02:56:15.0 PST"));
    assertThat(ts2.round(0).toString(2), is("1969-07-21 02:56:15.00 PST"));
  }

  private void checkTimestampWithLocalTimeZone(RexLiteral literal) {
    assertThat(literal,
        hasToString("1969-07-21 02:56:15:TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)"));
    assertThat(literal.getValue() instanceof TimestampString, is(true));
    assertThat(literal.getValue2() instanceof Long, is(true));
    assertThat(literal.getValue3() instanceof Long, is(true));
  }

  /** Tests
   * {@link RexBuilder#makeTimestampTzLiteral(TimestampWithTimeZoneString, int)}. */
  @Test void testTimestampTzLiterals() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType timestampType =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_TZ);
    final RelDataType timestampType3 =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_TZ, 3);
    final RelDataType timestampType9 =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_TZ, 9);
    final RelDataType timestampType18 =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_TZ, 18);
    final RexBuilder builder = new RexBuilder(typeFactory);

    // The new way
    final TimestampWithTimeZoneString ts =
        new TimestampWithTimeZoneString(1969, 7, 21, 2, 56, 15,
            TimeZone.getTimeZone("PST").getID());
    checkTimestampTz(builder.makeLiteral(ts, timestampType));

    // Now with milliseconds
    final TimestampWithTimeZoneString ts2 = ts.withMillis(56);
    assertThat(ts2, hasToString("1969-07-21 02:56:15.056 PST"));
    final RexLiteral literal2 =
        builder.makeLiteral(ts2, timestampType3);
    assertThat(literal2.getValue(), hasToString("1969-07-21 02:56:15.056 PST"));

    // Now with nanoseconds
    final TimestampWithTimeZoneString ts3 = ts.withNanos(56);
    final RexLiteral literal3 =
        builder.makeLiteral(ts3, timestampType9);
    assertThat(literal3.getValueAs(TimestampWithTimeZoneString.class),
        hasToString("1969-07-21 02:56:15 PST"));
    final TimestampWithTimeZoneString ts3b = ts.withNanos(2345678);
    final RexLiteral literal3b =
        builder.makeLiteral(ts3b, timestampType9);
    assertThat(literal3b.getValueAs(TimestampWithTimeZoneString.class),
        hasToString("1969-07-21 02:56:15.002 PST"));

    // Now with a very long fraction
    final TimestampWithTimeZoneString ts4 = ts.withFraction("102030405060708090102");
    final RexLiteral literal4 =
        builder.makeLiteral(ts4, timestampType18);
    assertThat(literal4.getValueAs(TimestampWithTimeZoneString.class),
        hasToString("1969-07-21 02:56:15.102 PST"));

    // toString
    assertThat(ts2.round(1), hasToString("1969-07-21 02:56:15 PST"));
    assertThat(ts2.round(2), hasToString("1969-07-21 02:56:15.05 PST"));
    assertThat(ts2.round(3), hasToString("1969-07-21 02:56:15.056 PST"));
    assertThat(ts2.round(4), hasToString("1969-07-21 02:56:15.056 PST"));

    assertThat(ts2.toString(6), is("1969-07-21 02:56:15.056000 PST"));
    assertThat(ts2.toString(1), is("1969-07-21 02:56:15.0 PST"));
    assertThat(ts2.toString(0), is("1969-07-21 02:56:15 PST"));

    assertThat(ts2.round(0), hasToString("1969-07-21 02:56:15 PST"));
    assertThat(ts2.round(0).toString(0), is("1969-07-21 02:56:15 PST"));
    assertThat(ts2.round(0).toString(1), is("1969-07-21 02:56:15.0 PST"));
    assertThat(ts2.round(0).toString(2), is("1969-07-21 02:56:15.00 PST"));
  }

  private void checkTimestampTz(RexLiteral literal) {
    assertThat(literal,
        hasToString("1969-07-21 02:56:15 PST:TIMESTAMP_TZ(0)"));
    assertThat(literal.getValue() instanceof TimestampWithTimeZoneString, is(true));
    assertThat(literal.getValue2() instanceof Long, is(true));
    assertThat(literal.getValue3() instanceof Long, is(true));
  }

  /** Tests {@link RexBuilder#makeTimeLiteral(TimeString, int)}. */
  @Test void testTimeLiteral() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType timeType = typeFactory.createSqlType(SqlTypeName.TIME);
    final RelDataType timeType3 =
        typeFactory.createSqlType(SqlTypeName.TIME, 3);
    final RelDataType timeType9 =
        typeFactory.createSqlType(SqlTypeName.TIME, 9);
    final RelDataType timeType18 =
        typeFactory.createSqlType(SqlTypeName.TIME, 18);
    final RexBuilder builder = new RexBuilder(typeFactory);

    // Old way: provide a Calendar
    final Calendar calendar = Util.calendar();
    calendar.set(1969, Calendar.JULY, 21, 2, 56, 15); // one small step
    calendar.set(Calendar.MILLISECOND, 0);
    checkTime(builder.makeLiteral(calendar, timeType));

    // Old way #2: Provide a Long
    checkTime(builder.makeLiteral(MOON_TIME, timeType));

    // The new way
    final TimeString t = new TimeString(2, 56, 15);
    assertThat(t.getMillisOfDay(), is(10575000));
    checkTime(builder.makeLiteral(t, timeType));

    // Now with milliseconds
    final TimeString t2 = t.withMillis(56);
    assertThat(t2.getMillisOfDay(), is(10575056));
    assertThat(t2, hasToString("02:56:15.056"));
    final RexLiteral literal2 = builder.makeLiteral(t2, timeType3);
    assertThat(literal2.getValueAs(TimeString.class),
        hasToString("02:56:15.056"));

    // Now with nanoseconds
    final TimeString t3 = t.withNanos(2345678);
    assertThat(t3.getMillisOfDay(), is(10575002));
    final RexLiteral literal3 = builder.makeLiteral(t3, timeType9);
    assertThat(literal3.getValueAs(TimeString.class),
        hasToString("02:56:15.002"));

    // Now with a very long fraction
    final TimeString t4 = t.withFraction("102030405060708090102");
    assertThat(t4.getMillisOfDay(), is(10575102));
    final RexLiteral literal4 = builder.makeLiteral(t4, timeType18);
    assertThat(literal4.getValueAs(TimeString.class),
        hasToString("02:56:15.102"));

    // toString
    assertThat(t2.round(1), hasToString("02:56:15"));
    assertThat(t2.round(2), hasToString("02:56:15.05"));
    assertThat(t2.round(3), hasToString("02:56:15.056"));
    assertThat(t2.round(4), hasToString("02:56:15.056"));

    assertThat(t2.toString(6), is("02:56:15.056000"));
    assertThat(t2.toString(1), is("02:56:15.0"));
    assertThat(t2.toString(0), is("02:56:15"));

    assertThat(t2.round(0), hasToString("02:56:15"));
    assertThat(t2.round(0).toString(0), is("02:56:15"));
    assertThat(t2.round(0).toString(1), is("02:56:15.0"));
    assertThat(t2.round(0).toString(2), is("02:56:15.00"));

    assertThat(TimeString.fromMillisOfDay(53560123),
        hasToString("14:52:40.123"));
  }

  private void checkTime(RexLiteral literal) {
    assertThat(literal, hasToString("02:56:15"));
    assertThat(literal.getValue() instanceof Calendar, is(true));
    assertThat(literal.getValue2() instanceof Integer, is(true));
    assertThat(literal.getValue3() instanceof Integer, is(true));
    assertThat((Integer) literal.getValue2(), is(MOON_TIME));
    assertThat(literal.getValueAs(Calendar.class), notNullValue());
    assertThat(literal.getValueAs(TimeString.class), notNullValue());
  }

  /** Tests {@link RexBuilder#makeDateLiteral(DateString)}. */
  @Test void testDateLiteral() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dateType = typeFactory.createSqlType(SqlTypeName.DATE);
    final RexBuilder builder = new RexBuilder(typeFactory);

    // Old way: provide a Calendar
    final Calendar calendar = Util.calendar();
    calendar.set(1969, Calendar.JULY, 21); // one small step
    calendar.set(Calendar.MILLISECOND, 0);
    checkDate(builder.makeLiteral(calendar, dateType));

    // Old way #2: Provide in Integer
    checkDate(builder.makeLiteral(MOON_DAY, dateType));

    // The new way
    final DateString d = new DateString(1969, 7, 21);
    checkDate(builder.makeLiteral(d, dateType));
  }

  private void checkDate(RexLiteral literal) {
    assertThat(literal, hasToString("1969-07-21"));
    assertThat(literal.getValue() instanceof Calendar, is(true));
    assertThat(literal.getValue2() instanceof Integer, is(true));
    assertThat(literal.getValue3() instanceof Integer, is(true));
    assertThat((Integer) literal.getValue2(), is(MOON_DAY));
    assertThat(literal.getValueAs(Calendar.class), notNullValue());
    assertThat(literal.getValueAs(DateString.class), notNullValue());
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2306">[CALCITE-2306]
   * AssertionError in {@link RexLiteral#getValue3} with null literal of type
   * DECIMAL</a>. */
  @Test void testDecimalLiteral() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType type = typeFactory.createSqlType(SqlTypeName.DECIMAL);
    final RexBuilder builder = new RexBuilder(typeFactory);
    final RexLiteral literal = builder.makeExactLiteral(null, type);
    assertThat(literal.getValue3(), nullValue());
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3587">[CALCITE-3587]
   * RexBuilder may lose decimal fraction for creating literal with DECIMAL type</a>.
   */
  @Test void testDecimal() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType type = typeFactory.createSqlType(SqlTypeName.DECIMAL, 4, 2);
    final RexBuilder builder = new RexBuilder(typeFactory);
    try {
      builder.makeLiteral(12.3, type);
      fail();
    } catch (AssertionError e) {
      assertThat(e.getMessage(),
          is("java.lang.Double is not compatible with DECIMAL, try to use makeExactLiteral"));
    }
  }

  /** Tests {@link RexBuilder#makeExactLiteral(BigDecimal, RelDataType)}. */
  @Test void testDecimalWithRoundingMode() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType type = typeFactory.createSqlType(SqlTypeName.DECIMAL, 4, 2);
    final RexBuilder builder = new RexBuilder(typeFactory);
    RexLiteral rexLiteral = builder.makeExactLiteral(new BigDecimal("13.556"), type);
    assertThat(rexLiteral.getValue() instanceof BigDecimal, is(true));
    assertThat(rexLiteral.getValue(), hasToString("13.55"));
    final RelDataTypeFactory typeFactoryHalfUp =
        new SqlTypeFactoryImpl(new RelDataTypeSystemImpl() {
          @Override public RoundingMode roundingMode() {
            return RoundingMode.HALF_UP;
          }
        });
    final RelDataType typeHalfUp =
        typeFactoryHalfUp.createSqlType(SqlTypeName.DECIMAL, 4, 2);
    final RexBuilder builderHalfUp = new RexBuilder(typeFactoryHalfUp);
    RexLiteral rexLiteralHalfUp =
        builderHalfUp.makeExactLiteral(new BigDecimal("13.556"), typeHalfUp);
    assertThat(rexLiteralHalfUp.getValue() instanceof BigDecimal, is(true));
    assertThat(rexLiteralHalfUp.getValue(), hasToString("13.56"));
  }

  @Test void testDecimalWithNegativeScaleRoundingHalfUp() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(new RelDataTypeSystemImpl() {
          @Override public int getMinScale(SqlTypeName typeName) {
            switch (typeName) {
            case DECIMAL:
              return -2;
            default:
              return super.getMinScale(typeName);
            }
          }

          @Override public RoundingMode roundingMode() {
            return RoundingMode.HALF_UP;
          }
        });
    final RelDataType type = typeFactory.createSqlType(SqlTypeName.DECIMAL, 3, -2);
    final RexBuilder builder = new RexBuilder(typeFactory);
    RexLiteral rexLiteral = builder.makeLiteral(new BigDecimal("12355"), type);
    assertThat(rexLiteral.getValue() instanceof BigDecimal, is(true));
    assertThat(rexLiteral.getValue(), hasToString("12400"));
  }

  @Test void testDecimalWithNegativeScaleRoundingDown() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(
            CustomTypeSystems.withMinScale(RelDataTypeSystem.DEFAULT,
                typeName -> -2));
    final RelDataType type = typeFactory.createSqlType(SqlTypeName.DECIMAL, 3, -2);
    final RexBuilder builder = new RexBuilder(typeFactory);
    RexLiteral rexLiteralHalfUp = builder.makeLiteral(new BigDecimal("12355"), type);
    assertThat(rexLiteralHalfUp.getValue() instanceof BigDecimal, is(true));
    assertThat(rexLiteralHalfUp.getValue(), hasToString("12300"));
  }

  /** Tests {@link DateString} year range. */
  @Test void testDateStringYearError() {
    try {
      final DateString dateString = new DateString(11969, 7, 21);
      fail("expected exception, got " + dateString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Year out of range: [11969]"));
    }
    try {
      final DateString dateString = new DateString("12345-01-23");
      fail("expected exception, got " + dateString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          containsString("Invalid date format: [12345-01-23]"));
    }
  }

  /** Tests {@link DateString} month range. */
  @Test void testDateStringMonthError() {
    try {
      final DateString dateString = new DateString(1969, 27, 21);
      fail("expected exception, got " + dateString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Month out of range: [27]"));
    }
    try {
      final DateString dateString = new DateString("1234-13-02");
      fail("expected exception, got " + dateString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Month out of range: [13]"));
    }
  }

  /** Tests {@link DateString} day range. */
  @Test void testDateStringDayError() {
    try {
      final DateString dateString = new DateString(1969, 7, 41);
      fail("expected exception, got " + dateString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Day out of range: [41]"));
    }
    try {
      final DateString dateString = new DateString("1234-01-32");
      fail("expected exception, got " + dateString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Day out of range: [32]"));
    }
    // We don't worry about the number of days in a month. 30 is in range.
    final DateString dateString = new DateString("1234-02-30");
    assertThat(dateString, notNullValue());
  }

  /** Tests {@link TimeString} hour range. */
  @Test void testTimeStringHourError() {
    try {
      final TimeString timeString = new TimeString(111, 34, 56);
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Hour out of range: [111]"));
    }
    try {
      final TimeString timeString = new TimeString("24:00:00");
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Hour out of range: [24]"));
    }
    try {
      final TimeString timeString = new TimeString("24:00");
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          containsString("Invalid time format: [24:00]"));
    }
  }

  /** Tests {@link TimeString} minute range. */
  @Test void testTimeStringMinuteError() {
    try {
      final TimeString timeString = new TimeString(12, 334, 56);
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Minute out of range: [334]"));
    }
    try {
      final TimeString timeString = new TimeString("12:60:23");
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Minute out of range: [60]"));
    }
  }

  /** Tests {@link TimeString} second range. */
  @Test void testTimeStringSecondError() {
    try {
      final TimeString timeString = new TimeString(12, 34, 567);
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Second out of range: [567]"));
    }
    try {
      final TimeString timeString = new TimeString(12, 34, -4);
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Second out of range: [-4]"));
    }
    try {
      final TimeString timeString = new TimeString("12:34:60");
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Second out of range: [60]"));
    }
  }

  /**
   * Test string literal encoding.
   */
  @Test void testStringLiteral() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType varchar =
        typeFactory.createSqlType(SqlTypeName.VARCHAR);
    final RexBuilder builder = new RexBuilder(typeFactory);

    final NlsString latin1 = new NlsString("foobar", "LATIN1", SqlCollation.IMPLICIT);
    final NlsString utf8 = new NlsString("foobar", "UTF8", SqlCollation.IMPLICIT);

    RexLiteral literal = builder.makePreciseStringLiteral("foobar");
    assertThat(literal, hasToString("'foobar'"));
    literal =
        builder.makePreciseStringLiteral(
            new ByteString(new byte[] { 'f', 'o', 'o', 'b', 'a', 'r'}),
            "UTF8", SqlCollation.IMPLICIT);
    assertThat(literal, hasToString("_UTF8'foobar'"));
    assertThat(literal.computeDigest(RexDigestIncludeType.ALWAYS),
        is("_UTF8'foobar':CHAR(6) CHARACTER SET \"UTF-8\""));
    literal =
        builder.makePreciseStringLiteral(
            new ByteString("\u82f1\u56fd".getBytes(StandardCharsets.UTF_8)),
            "UTF8", SqlCollation.IMPLICIT);
    assertThat(literal, hasToString("_UTF8'\u82f1\u56fd'"));
    // Test again to check decode cache.
    literal =
        builder.makePreciseStringLiteral(
            new ByteString("\u82f1".getBytes(StandardCharsets.UTF_8)),
            "UTF8", SqlCollation.IMPLICIT);
    assertThat(literal, hasToString("_UTF8'\u82f1'"));
    try {
      literal =
          builder.makePreciseStringLiteral(
              new ByteString("\u82f1\u56fd".getBytes(StandardCharsets.UTF_8)),
              "GB2312", SqlCollation.IMPLICIT);
      fail("expected exception, got " + literal);
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), containsString("Failed to encode"));
    }
    literal = builder.makeLiteral(latin1, varchar);
    assertThat(literal, hasToString("_LATIN1'foobar'"));
    literal = builder.makeLiteral(utf8, varchar);
    assertThat(literal, hasToString("_UTF8'foobar'"));
  }

  /** Tests {@link RexBuilder#makeExactLiteral(java.math.BigDecimal)}. */
  @Test void testBigDecimalLiteral() {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(new RelDataTypeSystemImpl() {
      @Override public int getMaxPrecision(SqlTypeName typeName) {
        return 38;
      }
    });
    final RexBuilder builder = new RexBuilder(typeFactory);
    checkBigDecimalLiteral(builder, "25");
    checkBigDecimalLiteral(builder, "9.9");
    checkBigDecimalLiteral(builder, "0");
    checkBigDecimalLiteral(builder, "-75.5");
    checkBigDecimalLiteral(builder, "10000000");
    checkBigDecimalLiteral(builder, "100000.111111111111111111");
    checkBigDecimalLiteral(builder, "-100000.111111111111111111");
    checkBigDecimalLiteral(builder, "73786976294838206464"); // 2^66
    checkBigDecimalLiteral(builder, "-73786976294838206464");
  }

  @Test void testMakeIn() {
    final RelDataTypeFactory typeFactory =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RelDataType floatType = typeFactory.createSqlType(SqlTypeName.REAL);
    RexNode left = rexBuilder.makeInputRef(floatType, 0);
    final RexNode literal1 = rexBuilder.makeLiteral(1.0f, floatType);
    final RexNode literal2 = rexBuilder.makeLiteral(2.0f, floatType);
    RexNode inCall = rexBuilder.makeIn(left, ImmutableList.of(literal1, literal2));
    assertThat(inCall.getKind(), is(SqlKind.SEARCH));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6989">[CALCITE-6989]
   * Enhance RexBuilder#makeIn to create SEARCH for ARRAY literals</a>.
   */
  @Test void testMakeInReturnsSearchForArrayLiterals() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType arrayIntType = typeFactory.createArrayType(intType, -1);
    RexNode column = rexBuilder.makeInputRef(arrayIntType, 0);
    RexNode l1 = rexBuilder.makeLiteral(ImmutableList.of(100, 200), arrayIntType, false);
    RexNode l2 = rexBuilder.makeLiteral(ImmutableList.of(300, 400), arrayIntType, false);
    RexNode inCall = rexBuilder.makeIn(column, ImmutableList.of(l1, l2));
    assertThat(
        inCall, hasToString("SEARCH($0, Sarg["
        + "[100:INTEGER, 200:INTEGER]:INTEGER NOT NULL ARRAY, "
        + "[300:INTEGER, 400:INTEGER]:INTEGER NOT NULL ARRAY"
        + "]:INTEGER NOT NULL ARRAY)"));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6608">[CALCITE-6608]
   * RexBuilder#makeIn should create EQUALS instead of SEARCH for single point values</a>.
   */
  @Test void testMakeInReturnsEqualsForSingleLiteral() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RexNode column = rexBuilder.makeInputRef(intType, 0);
    RexLiteral literal = rexBuilder.makeLiteral(100, intType);
    RexNode inCall = rexBuilder.makeIn(column, ImmutableList.of(literal));
    assertThat(inCall, hasToString("=($0, 100)"));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6608">[CALCITE-6608]
   * RexBuilder#makeIn should create EQUALS instead of SEARCH for single point values</a>.
   */
  @Test void testMakeInReturnsEqualsForDuplicateLiterals() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RexNode column = rexBuilder.makeInputRef(intType, 0);
    RexLiteral literal = rexBuilder.makeLiteral(100, intType);
    RexNode inCall = rexBuilder.makeIn(column, ImmutableList.of(literal, literal));
    assertThat(inCall, hasToString("=($0, 100)"));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6608">[CALCITE-6608]
   * RexBuilder#makeIn should create EQUALS instead of SEARCH for single point values</a>.
   */
  @Test void testMakeInReturnsEqualsForSingleExpression() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RexNode column0 = rexBuilder.makeInputRef(intType, 0);
    RexNode plusCall =
        rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeInputRef(intType, 2));
    RexNode inCall = rexBuilder.makeIn(column0, ImmutableList.of(plusCall));
    assertThat(inCall, hasToString("=($0, +($1, $2))"));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6608">[CALCITE-6608]
   * RexBuilder#makeIn should create EQUALS instead of SEARCH for single point values</a>.
   */
  @Test void testMakeInReturnsEqualsForDuplicateExpressions() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RexNode column0 = rexBuilder.makeInputRef(intType, 0);
    RexNode plusCall =
        rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeInputRef(intType, 2));
    RexNode inCall = rexBuilder.makeIn(column0, ImmutableList.of(plusCall, plusCall));
    assertThat(inCall, hasToString("=($0, +($1, $2))"));
  }

  @Test void testMakeInReturnsOrForMultipleExpressions() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RexNode column0 = rexBuilder.makeInputRef(intType, 0);
    RexNode plusCall =
        rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeInputRef(intType, 2));
    RexNode minusCall =
        rexBuilder.makeCall(SqlStdOperatorTable.MINUS,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeInputRef(intType, 2));
    RexNode inCall = rexBuilder.makeIn(column0, ImmutableList.of(plusCall, minusCall));
    assertThat(inCall, hasToString("OR(=($0, +($1, $2)), =($0, -($1, $2)))"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4555">[CALCITE-4555]
   * Invalid zero literal value is used for
   * TIMESTAMP WITH LOCAL TIME ZONE type in RexBuilder</a>. */
  @ParameterizedTest
  @MethodSource("testData4testMakeZeroLiteral")
  void testMakeZeroLiteral(RelDataType type, RexLiteral expected) {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    assertThat(rexBuilder.makeZeroLiteral(type), is(equalTo(expected)));
  }

  private static Stream<Arguments> testData4testMakeZeroLiteral() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    BiFunction<RelDataType, Function<RelDataType, Comparable>, Arguments> type2rexLiteral =
        (relDataType, relDataTypeComparableFunction) ->
            of(relDataType,
                rexBuilder.makeLiteral(
                    relDataTypeComparableFunction.apply(relDataType), relDataType));
    return Stream.of(
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.CHAR),
            relDataType -> new NlsString(Spaces.of(relDataType.getPrecision()), null, null)),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.VARCHAR),
            relDataType -> new NlsString("", null, null)),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.BINARY),
            relDataType -> new ByteString(new byte[relDataType.getPrecision()])),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.VARBINARY),
            relDataType -> ByteString.EMPTY),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.TINYINT),
            relDataType -> BigDecimal.ZERO),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.SMALLINT),
            relDataType -> BigDecimal.ZERO),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.INTEGER),
            relDataType -> BigDecimal.ZERO),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.BIGINT),
            relDataType -> BigDecimal.ZERO),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.DECIMAL),
            relDataType -> BigDecimal.ZERO),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.FLOAT),
            relDataType -> BigDecimal.ZERO),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.REAL),
            relDataType -> BigDecimal.ZERO),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.DOUBLE),
            relDataType -> BigDecimal.ZERO),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.BOOLEAN),
            relDataType -> false),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.TIME),
            relDataType -> DateTimeUtils.ZERO_CALENDAR),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.DATE),
            relDataType -> DateTimeUtils.ZERO_CALENDAR),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
            relDataType -> DateTimeUtils.ZERO_CALENDAR),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE),
            relDataType -> new TimeString(0, 0, 0)),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
            relDataType -> new TimestampString(0, 1, 1, 0, 0, 0)),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.TIME_TZ),
            relDataType -> new TimeWithTimeZoneString(0, 0, 0, "GMT+00:00")),
        type2rexLiteral.apply(typeFactory.createSqlType(SqlTypeName.TIMESTAMP_TZ),
            relDataType -> new TimestampWithTimeZoneString(0, 1, 1, 0, 0, 0, "GMT+00:00")));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6938">[CALCITE-6938]
   * Support zero value creation of nested data types</a>. */
  @ParameterizedTest
  @MethodSource("testData4testMakeZeroForNestedType")
  void testMakeZeroForNestedType(RelDataType type, RexNode expected) {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    assertThat(rexBuilder.makeZeroRexNode(type), is(equalTo(expected)));
  }

  @Test void testCreateCoalesce() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder b = new RexBuilder(typeFactory);
    RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

    RelDataType arrayType = new ArraySqlType(varcharType, false);
    RexNode arrayZero = b.makeZeroRexNode(arrayType);

    RexNode array =
        b.makeCall(arrayType, SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
        ImmutableList.of(
            b.makeLiteral("1", varcharType)));

    RexNode coalesce1 = b.makeCall(SqlStdOperatorTable.COALESCE, array, arrayZero);
    assertThat(
        coalesce1, hasToString(
        "COALESCE(ARRAY('1'), CAST(ARRAY()):VARCHAR NOT NULL ARRAY NOT NULL)"));

    RelDataType mapType = new MapSqlType(arrayType, arrayType, true);
    RexNode mapZero = b.makeZeroRexNode(mapType);

    RexNode map =
        b.makeCall(new MapSqlType(arrayType, arrayType, true),
        SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
        ImmutableList.of(array, array));

    RexNode coalesce2 = b.makeCall(SqlStdOperatorTable.COALESCE, map, mapZero);
    assertThat(
        coalesce2, hasToString(
        "COALESCE(MAP(ARRAY('1'), ARRAY('1')), "
        + "CAST(MAP()):(VARCHAR NOT NULL ARRAY NOT NULL, VARCHAR NOT NULL ARRAY NOT NULL) MAP)"));
  }

  private static Stream<Arguments> testData4testMakeZeroForNestedType() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder b = new RexBuilder(typeFactory);

    RelDataType integerType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

    // ARRAY<INTEGER>
    RelDataType arrayType = new ArraySqlType(integerType, false);
    RexNode expectedArray =
        b.makeCast(
            arrayType, b.makeCall(arrayType, SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
            ImmutableList.of()));

    // MULTISET<INTEGER>
    RelDataType multisetType = new MultisetSqlType(integerType, false);
    RexNode expectedMultiset =
        b.makeCast(
            multisetType, b.makeCall(multisetType, SqlStdOperatorTable.MULTISET_VALUE,
            ImmutableList.of()));

    // MAP<VARCHAR, INTEGER>
    RelDataType mapType = new MapSqlType(varcharType, integerType, false);
    RexNode expectedMap =
        b.makeCast(
            mapType, b.makeCall(mapType, SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            ImmutableList.of()));

    // ROW<INTEGER, VARCHAR>
    RelDataType rowType =
        typeFactory.createStructType(
            ImmutableList.of(
            new RelDataTypeFieldImpl("integer", 0, integerType),
            new RelDataTypeFieldImpl("varchar", 1, varcharType)));
    RexNode expectedRow =
        b.makeCall(rowType, SqlStdOperatorTable.ROW,
            ImmutableList.of(b.makeZeroLiteral(integerType),
                b.makeZeroLiteral(varcharType)));

    // ARRAY<ARRAY<INTEGER>>
    RelDataType arrayArrayType = new ArraySqlType(arrayType, false);
    RexNode expectedArrayArray =
        b.makeCast(
            arrayArrayType, b.makeCall(arrayArrayType, SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
            ImmutableList.of()));

    // ARRAY<MAP<VARCHAR, INTEGER>>
    RelDataType arrayMapType = new ArraySqlType(mapType, false);
    RexNode expectedArrayMap =
        b.makeCast(
            arrayMapType, b.makeCall(arrayMapType, SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
            ImmutableList.of()));

    // MAP<MAP<INTEGER, INTEGER>
    RelDataType mapMapType = new MapSqlType(mapType, integerType, false);
    RexNode expectedMapMap =
        b.makeCast(
            mapMapType, b.makeCall(mapMapType, SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            ImmutableList.of()));

    // MAP<ARRAY<INTEGER>, INTEGER>
    RelDataType mapArrayType = new MapSqlType(arrayType, integerType, false);
    RexNode expectedMapArray =
        b.makeCast(
            mapArrayType, b.makeCall(mapArrayType, SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            ImmutableList.of()));

    // ROW<ARRAY<INTEGER>, VARCHAR>
    RelDataType rowArrayType =
        typeFactory.createStructType(
            ImmutableList.of(
                new RelDataTypeFieldImpl("array", 0, arrayType),
                new RelDataTypeFieldImpl("varchar", 1, varcharType)));
    RexNode expectedRowArray =
        b.makeCall(rowArrayType, SqlStdOperatorTable.ROW,
            ImmutableList.of(expectedArray,
                b.makeZeroLiteral(varcharType)));

    return Stream.of(
        Arguments.of(arrayType, expectedArray),
        Arguments.of(multisetType, expectedMultiset),
        Arguments.of(mapType, expectedMap),
        Arguments.of(rowType, expectedRow),
        Arguments.of(arrayArrayType, expectedArrayArray),
        Arguments.of(arrayMapType, expectedArrayMap),
        Arguments.of(mapMapType, expectedMapMap),
        Arguments.of(mapArrayType, expectedMapArray),
        Arguments.of(rowArrayType, expectedRowArray));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4632">[CALCITE-4632]
   * Find the least restrictive datatype for SARG</a>. */
  @Test void testLeastRestrictiveTypeForSargMakeIn() {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RelDataType decimalType = typeFactory.createSqlType(SqlTypeName.DECIMAL);
    RexNode left = rexBuilder.makeInputRef(decimalType, 0);
    final RexNode literal1 = rexBuilder.makeExactLiteral(new BigDecimal("1.0"));
    final RexNode literal2 = rexBuilder.makeExactLiteral(new BigDecimal("20000.0"));

    RexNode inCall = rexBuilder.makeIn(left, ImmutableList.of(literal1, literal2));
    assertThat(inCall.getKind(), is(SqlKind.SEARCH));

    final RexNode sarg = ((RexCall) inCall).operands.get(1);
    RelDataType expected = typeFactory.createSqlType(SqlTypeName.DECIMAL, 6, 1);
    assertThat(expected, is(sarg.getType()));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4632">[CALCITE-4632]
   * Find the least restrictive datatype for SARG</a>. */
  @Test void testLeastRestrictiveTypeForSargMakeBetween() {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RelDataType decimalType = typeFactory.createSqlType(SqlTypeName.DECIMAL);
    RexNode left = rexBuilder.makeInputRef(decimalType, 0);
    final RexNode literal1 = rexBuilder.makeExactLiteral(new BigDecimal("1.0"));
    final RexNode literal2 = rexBuilder.makeExactLiteral(new BigDecimal("20000.0"));

    RexNode betweenCall = rexBuilder.makeBetween(left, literal1, literal2);
    assertThat(betweenCall.getKind(), is(SqlKind.SEARCH));

    final RexNode sarg = ((RexCall) betweenCall).operands.get(1);
    RelDataType expected = typeFactory.createSqlType(SqlTypeName.DECIMAL, 6, 1);
    assertThat(expected, is(sarg.getType()));
  }

  /** Tests {@link RexCopier#visitOver(RexOver)}. */
  @Test void testCopyOver() {
    final RelDataTypeFactory sourceTypeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = sourceTypeFactory.createSqlType(SqlTypeName.VARCHAR, 65536);

    final RelDataTypeFactory targetTypeFactory =
        new MySqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder builder = new RexBuilder(targetTypeFactory);

    final RexOver node =
        (RexOver) builder.makeOver(type, SqlStdOperatorTable.COUNT,
            ImmutableList.of(builder.makeInputRef(type, 0)),
            ImmutableList.of(builder.makeInputRef(type, 1)),
            ImmutableList.of(
                new RexFieldCollation(builder.makeInputRef(type, 2),
                    ImmutableSet.of())),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true, true, false, false, false);
    final RexNode copy = builder.copy(node);
    assertThat(copy, instanceOf(RexOver.class));

    RexOver result = (RexOver) copy;
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.VARCHAR));
    assertThat(result.getType().getPrecision(), is(PRECISION));
    assertThat(result.getWindow(), is(node.getWindow()));
    assertThat(result.getAggOperator(), is(node.getAggOperator()));
    assertThat(result.getAggOperator(), is(node.getAggOperator()));
    assertThat(result.isDistinct(), is(node.isDistinct()));
    assertThat(result.ignoreNulls(), is(node.ignoreNulls()));
    for (int i = 0; i < node.getOperands().size(); i++) {
      assertThat(result.getOperands().get(i).getType().getSqlTypeName(),
          is(node.getOperands().get(i).getType().getSqlTypeName()));
      assertThat(result.getOperands().get(i).getType().getPrecision(),
          is(PRECISION));
    }
  }

  /** Tests {@link RexCopier#visitCorrelVariable(RexCorrelVariable)}. */
  @Test void testCopyCorrelVariable() {
    final RelDataTypeFactory sourceTypeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = sourceTypeFactory.createSqlType(SqlTypeName.VARCHAR, 65536);

    final RelDataTypeFactory targetTypeFactory =
        new MySqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder builder = new RexBuilder(targetTypeFactory);

    final RexCorrelVariable node =
        (RexCorrelVariable) builder.makeCorrel(type, new CorrelationId(0));
    final RexNode copy = builder.copy(node);
    assertThat(copy, instanceOf(RexCorrelVariable.class));

    final RexCorrelVariable result = (RexCorrelVariable) copy;
    assertThat(result.id, is(node.id));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.VARCHAR));
    assertThat(result.getType().getPrecision(), is(PRECISION));
  }

  /** Tests {@link RexCopier#visitLocalRef(RexLocalRef)}. */
  @Test void testCopyLocalRef() {
    final RelDataTypeFactory sourceTypeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = sourceTypeFactory.createSqlType(SqlTypeName.VARCHAR, 65536);

    final RelDataTypeFactory targetTypeFactory =
        new MySqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder builder = new RexBuilder(targetTypeFactory);

    final RexLocalRef node = new RexLocalRef(0, type);
    final RexNode copy = builder.copy(node);
    assertThat(copy, instanceOf(RexLocalRef.class));

    final RexLocalRef result = (RexLocalRef) copy;
    assertThat(result.getIndex(), is(node.getIndex()));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.VARCHAR));
    assertThat(result.getType().getPrecision(), is(PRECISION));
  }

  /** Tests {@link RexCopier#visitDynamicParam(RexDynamicParam)}. */
  @Test void testCopyDynamicParam() {
    final RelDataTypeFactory sourceTypeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = sourceTypeFactory.createSqlType(SqlTypeName.VARCHAR, 65536);

    final RelDataTypeFactory targetTypeFactory =
        new MySqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder builder = new RexBuilder(targetTypeFactory);

    final RexDynamicParam node = builder.makeDynamicParam(type, 0);
    final RexNode copy = builder.copy(node);
    assertThat(copy, instanceOf(RexDynamicParam.class));

    final RexDynamicParam result = (RexDynamicParam) copy;
    assertThat(result.getIndex(), is(node.getIndex()));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.VARCHAR));
    assertThat(result.getType().getPrecision(), is(PRECISION));
  }

  /** Tests {@link RexCopier#visitRangeRef(RexRangeRef)}. */
  @Test void testCopyRangeRef() {
    final RelDataTypeFactory sourceTypeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = sourceTypeFactory.createSqlType(SqlTypeName.VARCHAR, 65536);

    final RelDataTypeFactory targetTypeFactory =
        new MySqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder builder = new RexBuilder(targetTypeFactory);

    final RexRangeRef node = builder.makeRangeReference(type, 1, true);
    final RexNode copy = builder.copy(node);
    assertThat(copy, instanceOf(RexRangeRef.class));

    final RexRangeRef result = (RexRangeRef) copy;
    assertThat(result.getOffset(), is(node.getOffset()));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.VARCHAR));
    assertThat(result.getType().getPrecision(), is(PRECISION));
  }

  private void checkBigDecimalLiteral(RexBuilder builder, String val) {
    final RexLiteral literal = builder.makeExactLiteral(new BigDecimal(val));
    assertThat("builder.makeExactLiteral(new BigDecimal(" + val
            + ")).getValueAs(BigDecimal.class).toString()",
        literal.getValueAs(BigDecimal.class), hasToString(val));
  }

  @Test void testValidateRexFieldAccess() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder builder = new RexBuilder(typeFactory);

    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType longType = typeFactory.createSqlType(SqlTypeName.BIGINT);

    RelDataType structType =
        typeFactory.createStructType(Arrays.asList(intType, longType),
            Arrays.asList("x", "y"));
    RexInputRef inputRef = builder.makeInputRef(structType, 0);

    // construct RexFieldAccess fails because of negative index
    IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class, () -> {
      RelDataTypeField field = new RelDataTypeFieldImpl("z", -1, intType);
      new RexFieldAccess(inputRef, field);
    });
    assertThat(e1.getMessage(),
        is("Field #-1: z INTEGER does not exist for expression $0"));

    // construct RexFieldAccess fails because of too large index
    IllegalArgumentException e2 = assertThrows(IllegalArgumentException.class, () -> {
      RelDataTypeField field = new RelDataTypeFieldImpl("z", 2, intType);
      new RexFieldAccess(inputRef, field);
    });
    assertThat(e2.getMessage(),
        is("Field #2: z INTEGER does not exist for expression $0"));

    // construct RexFieldAccess fails because of incorrect type
    IllegalArgumentException e3 = assertThrows(IllegalArgumentException.class, () -> {
      RelDataTypeField field = new RelDataTypeFieldImpl("z", 0, longType);
      new RexFieldAccess(inputRef, field);
    });
    assertThat(e3.getMessage(),
        is("Field #0: z BIGINT does not exist for expression $0"));

    // construct RexFieldAccess successfully
    RelDataTypeField field = new RelDataTypeFieldImpl("x", 0, intType);
    RexFieldAccess fieldAccess = new RexFieldAccess(inputRef, field);
    RexChecker checker = new RexChecker(structType, () -> null, Litmus.THROW);
    assertThat(fieldAccess.accept(checker), is(true));
  }

  /** Emulate a user defined type. */
  private static class UDT extends RelDataTypeImpl {
    UDT() {
      this.digest = "(udt)NOT NULL";
    }

    @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
      sb.append("udt");
    }
  }

  @Test void testUDTLiteralDigest() {
    RexLiteral literal = new RexLiteral(new BigDecimal(0L), new UDT(), SqlTypeName.BIGINT);

    // when the space before "NOT NULL" is missing, the digest is not correct
    // and the suffix should not be removed.
    assertThat(literal.digest, is("0L:(udt)NOT NULL"));
  }


  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5489">[CALCITE-5489]
   * RexCall to TIMESTAMP_DIFF function fails to convert a TIMESTAMP literal to
   * a org.apache.calcite.avatica.util.TimeUnit</a>. */
  @Test void testTimestampDiffCall() {
    final RexImplicationCheckerFixtures.Fixture f =
        new RexImplicationCheckerFixtures.Fixture();
    final TimestampString ts =
        TimestampString.fromCalendarFields(Util.calendar());
    final RexNode literal = f.timestampLiteral(ts);
    final RexLiteral flag = f.rexBuilder.makeFlag(TimeUnit.QUARTER);
    assertThat(
        f.rexBuilder.makeCall(SqlLibraryOperators.DATEDIFF,
            flag, literal, literal),
        notNullValue());
    assertThat(
        f.rexBuilder.makeCall(SqlStdOperatorTable.TIMESTAMP_DIFF,
            flag, literal, literal),
        notNullValue());
    assertThat(
        f.rexBuilder.makeCall(SqlLibraryOperators.TIMESTAMP_DIFF3,
            literal, literal, flag),
        notNullValue());
    assertThat(
        f.rexBuilder.makeCall(SqlLibraryOperators.TIME_DIFF,
            literal, literal, flag),
        notNullValue());
  }
}
