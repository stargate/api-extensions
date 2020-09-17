/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.api.sql;

import static io.stargate.db.datastore.schema.Column.Kind.PartitionKey;
import static io.stargate.db.datastore.schema.Column.Kind.Regular;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.matches;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.stargate.api.sql.schema.TypeUtils;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.PreparedStatement;
import io.stargate.db.datastore.query.QueryBuilder;
import io.stargate.db.datastore.schema.Column;
import io.stargate.db.datastore.schema.ImmutableColumn;
import io.stargate.db.datastore.schema.ImmutableKeyspace;
import io.stargate.db.datastore.schema.ImmutableSchema;
import io.stargate.db.datastore.schema.ImmutableTable;
import io.stargate.db.datastore.schema.Keyspace;
import io.stargate.db.datastore.schema.Schema;
import io.stargate.db.datastore.schema.Table;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.internal.matchers.AnyVararg;

public class AbstractDataStoreTest {

  public static final BigDecimal BIG_DECIMAL_EXAMPLE =
      BigDecimal.valueOf(Long.MAX_VALUE).multiply(BigDecimal.TEN).add(BigDecimal.valueOf(0.12345d));

  protected static final Table table1 =
      ImmutableTable.builder()
          .keyspace("test_ks")
          .name("test1")
          .addColumns(
              ImmutableColumn.builder().name("a").type(Column.Type.Int).kind(PartitionKey).build())
          .build();

  protected static final Table table2 =
      ImmutableTable.builder()
          .keyspace("test_ks")
          .name("test2")
          .addColumns(
              ImmutableColumn.builder().name("x").type(Column.Type.Int).kind(PartitionKey).build())
          .addColumns(
              ImmutableColumn.builder().name("y").type(Column.Type.Text).kind(Regular).build())
          .build();

  protected static final Table table2a =
      ImmutableTable.builder()
          .keyspace("test_ks")
          .name("test2a")
          .addColumns(
              ImmutableColumn.builder().name("x1").type(Column.Type.Int).kind(PartitionKey).build())
          .addColumns(
              ImmutableColumn.builder().name("x2").type(Column.Type.Int).kind(PartitionKey).build())
          .addColumns(
              ImmutableColumn.builder().name("y").type(Column.Type.Text).kind(Regular).build())
          .build();

  protected static final Table table3 =
      ImmutableTable.builder()
          .keyspace("test_ks")
          .name("supported_types")
          .addColumns(
              ImmutableColumn.builder()
                  .kind(PartitionKey)
                  .name("pk")
                  .type(Column.Type.Ascii)
                  .build())
          .addColumns(
              ImmutableColumn.builder()
                  .kind(Regular)
                  .name("c_ascii")
                  .type(Column.Type.Ascii)
                  .build())
          .addColumns(
              ImmutableColumn.builder()
                  .kind(Regular)
                  .name("c_bigint")
                  .type(Column.Type.Bigint)
                  .build())
          // TODO:
          // .addColumns(ImmutableColumn.builder().name("c_blob").type(Column.Type.Blob).build())
          .addColumns(
              ImmutableColumn.builder()
                  .kind(Regular)
                  .name("c_boolean")
                  .type(Column.Type.Boolean)
                  .build())
          .addColumns(
              ImmutableColumn.builder()
                  .kind(Regular)
                  .name("c_counter")
                  .type(Column.Type.Counter)
                  .build())
          .addColumns(
              ImmutableColumn.builder().kind(Regular).name("c_date").type(Column.Type.Date).build())
          .addColumns(
              ImmutableColumn.builder()
                  .kind(Regular)
                  .name("c_decimal")
                  .type(Column.Type.Decimal)
                  .build())
          .addColumns(
              ImmutableColumn.builder()
                  .kind(Regular)
                  .name("c_double")
                  .type(Column.Type.Double)
                  .build())
          .addColumns(
              ImmutableColumn.builder()
                  .kind(Regular)
                  .name("c_duration")
                  .type(Column.Type.Duration)
                  .build())
          // TODO: check for bugs in Avatica's handling of float values,
          // cf. org.apache.calcite.avatica.remote.TypedValue.writeToProtoWithType, line 805
          //     `writeToProtoWithType(builder, ((Float) o).longValue(), Common.Rep.FLOAT);`
          // TODO:
          // .addColumns(ImmutableColumn.builder().name("c_float").type(Column.Type.Float).build())
          .addColumns(
              ImmutableColumn.builder().kind(Regular).name("c_inet").type(Column.Type.Inet).build())
          .addColumns(
              ImmutableColumn.builder().kind(Regular).name("c_int").type(Column.Type.Int).build())
          .addColumns(
              ImmutableColumn.builder()
                  .kind(Regular)
                  .name("c_smallint")
                  .type(Column.Type.Smallint)
                  .build())
          .addColumns(
              ImmutableColumn.builder().kind(Regular).name("c_text").type(Column.Type.Text).build())
          .addColumns(
              ImmutableColumn.builder().kind(Regular).name("c_time").type(Column.Type.Time).build())
          .addColumns(
              ImmutableColumn.builder()
                  .kind(Regular)
                  .name("c_timestamp")
                  .type(Column.Type.Timestamp)
                  .build())
          .addColumns(
              ImmutableColumn.builder()
                  .kind(Regular)
                  .name("c_timeuuid")
                  .type(Column.Type.Timeuuid)
                  .build())
          .addColumns(
              ImmutableColumn.builder()
                  .kind(Regular)
                  .name("c_tinyint")
                  .type(Column.Type.Tinyint)
                  .build())
          .addColumns(
              ImmutableColumn.builder().kind(Regular).name("c_uuid").type(Column.Type.Uuid).build())
          // TODO:.addColumns(ImmutableColumn.builder().name("c_varint").type(Column.Type.Varint).build())
          .build();

  private static final Keyspace keyspace =
      ImmutableKeyspace.builder()
          .name("test_ks")
          .addTables(table1)
          .addTables(table2)
          .addTables(table2a)
          .addTables(table3)
          .build();

  private static final Schema schema = ImmutableSchema.builder().addKeyspaces(keyspace).build();

  protected final DataStore dataStore;
  private final Collection<ExpectedExecution> expectedExecutions = new ArrayList<>();

  public AbstractDataStoreTest() {
    dataStore = Mockito.mock(DataStore.class);
    Mockito.when(dataStore.schema()).thenReturn(schema);

    Mockito.when(dataStore.query()).thenAnswer(invocation -> new QueryBuilder(dataStore));

    withQuery(table1, "SELECT a FROM %s")
        .returning(ImmutableList.of(ImmutableMap.of("a", 20), ImmutableMap.of("a", 10)));

    withQuery(table2, "SELECT x, y FROM %s")
        .returning(
            ImmutableList.of(
                ImmutableMap.of("x", 1, "y", "row_1"), ImmutableMap.of("x", 2, "y", "row_2")));

    withAnySelectFrom(table3).returning(ImmutableList.of(sampleValues(table3, false)));
    withAnyInsertInfo(table3).returningNothing();
    withAnyUpdateOf(table3).returningNothing();
  }

  protected static Map<String, Object> sampleValues(Table table, boolean client) {
    return table.columns().stream()
        .collect(
            Collectors.toMap(
                Column::name,
                c -> {
                  try {
                    Column.ColumnType type = c.type();
                    Assertions.assertThat(type).isNotNull();
                    Class<?> javaType = type.javaType();

                    Object rawValue = null;
                    if (Integer.class.isAssignableFrom(javaType)) {
                      rawValue = Integer.MAX_VALUE;
                    } else if (Short.class.isAssignableFrom(javaType)) {
                      rawValue = Short.MAX_VALUE;
                    } else if (Byte.class.isAssignableFrom(javaType)) {
                      rawValue = Byte.MAX_VALUE;
                    } else if (Long.class.isAssignableFrom(javaType)) {
                      rawValue = Long.MAX_VALUE;
                    } else if (BigDecimal.class.isAssignableFrom(javaType)) {
                      rawValue = BIG_DECIMAL_EXAMPLE;
                    } else if (Double.class.isAssignableFrom(javaType)) {
                      rawValue = Double.MAX_VALUE;
                    } else if (Float.class.isAssignableFrom(javaType)) {
                      rawValue = Float.MAX_VALUE;
                    } else if (String.class.isAssignableFrom(javaType)) {
                      rawValue = "example";
                    } else if (ByteBuffer.class.isAssignableFrom(javaType)) {
                      rawValue = ByteBuffer.wrap(new byte[] {1});
                    } else if (Boolean.class.isAssignableFrom(javaType)) {
                      rawValue = false;
                    } else if (LocalDate.class.isAssignableFrom(javaType)) {
                      rawValue = LocalDate.of(2020, 1, 2);
                    } else if (LocalTime.class.isAssignableFrom(javaType)) {
                      rawValue = LocalTime.of(23, 42, 11);
                    } else if (Instant.class.isAssignableFrom(javaType)) {
                      rawValue = Instant.ofEpochMilli(0);
                    } else if (CqlDuration.class.isAssignableFrom(javaType)) {
                      rawValue = CqlDuration.from("1mo2d3s");
                    } else if (InetAddress.class.isAssignableFrom(javaType)) {
                      rawValue = InetAddress.getLoopbackAddress();
                    } else if (UUID.class.isAssignableFrom(javaType)) {
                      rawValue = Uuids.startOf(1);
                    }

                    if (rawValue == null) {
                      throw new IllegalStateException("Unsupported type: " + type);
                    }

                    return client ? TypeUtils.toJdbcValue(rawValue, type) : rawValue;
                  } catch (Exception e) {
                    throw new IllegalStateException(e);
                  }
                }));
  }

  protected QueryExpectation withQuery(Table table, String cql, Object... params) {
    cql = String.format(cql, keyspace.name() + "." + table.name());
    return new QueryExpectation(table, Pattern.quote(cql), params);
  }

  protected QueryExpectation withAnySelectFrom(Table table) {
    String regex = "SELECT.*FROM.*" + keyspace.name() + "\\." + table.name() + ".*";
    return new QueryExpectation(table, regex, new Object[0]);
  }

  protected QueryExpectation withAnyUpdateOf(Table table) {
    String regex = "UPDATE.*" + keyspace.name() + "\\." + table.name() + ".*";
    return new QueryExpectation(table, regex);
  }

  protected QueryExpectation withAnyInsertInfo(Table table) {
    String regex = "INSERT INTO.*" + keyspace.name() + "\\." + table.name() + ".*";
    return new QueryExpectation(table, regex);
  }

  protected QueryExpectation withAnyDeleteFrom(Table table) {
    String regex = "DELETE.*FROM.*" + keyspace.name() + "\\." + table.name() + ".*";
    return new QueryExpectation(table, regex);
  }

  protected void withNoOtherCqlStatements() {
    Mockito.when(dataStore.prepare(any(), any()))
        .thenAnswer(
            inv -> {
              Object cql = inv.getArguments()[0];
              Assertions.fail("Unexpected prepare call: " + cql);
              return null;
            });
  }

  @BeforeEach
  public void clearExpectedExecutions() {
    expectedExecutions.clear();
  }

  @AfterEach
  public void checkExpectedExecutions() {
    expectedExecutions.forEach(ExpectedExecution::validate);
  }

  protected void ignorePreparedExecutions() {
    expectedExecutions.clear();
  }

  protected class QueryExpectation {

    private final Table table;
    private final String cqlRegEx;
    private final Object[] params;
    private final ArgumentMatcher<?> matchesParams;

    private QueryExpectation(Table table, String cqlRegEx, Object[] params) {
      this.cqlRegEx = cqlRegEx;
      this.table = table;
      this.params = params;
      matchesParams =
          new AnyVararg() {
            @Override
            public boolean matches(Object arg) {
              return CoreMatchers.equalTo(params).matches(arg);
            }
          };
    }

    private QueryExpectation(Table table, String cqlRegEx) {
      this.cqlRegEx = cqlRegEx;
      this.table = table;
      this.params = new Object[0];
      matchesParams = new AnyVararg();
    }

    public void returningNothing() {
      returning(Collections.emptyList());
    }

    public void returning(List<Map<String, Object>> rows) {
      final ExpectedExecution expectedExecution = new ExpectedExecution(this);

      PreparedStatement prepared = Mockito.mock(PreparedStatement.class);
      Mockito.when(prepared.execute(eq(dataStore), any(), Mockito.argThat(matchesParams)))
          .thenAnswer(
              inv -> {
                // for later verification by test code
                dataStore.query(expectedExecution.actualCql, params);

                expectedExecution.executed();
                return CompletableFuture.completedFuture(ListBackedResultSet.of(table, rows));
              });

      Mockito.when(dataStore.prepare(matches(cqlRegEx), any()))
          .thenAnswer(
              inv -> {
                expectedExecution.prepared((String) inv.getArguments()[0]);
                return prepared;
              });
    }
  }

  private class ExpectedExecution {

    private final QueryExpectation query;
    private String actualCql;
    private boolean executed;

    private ExpectedExecution(QueryExpectation query) {
      this.query = query;
    }

    private void validate() {
      Assertions.assertThat(
              Arrays.asList(
                  "The following query was prepared but not executed", actualCql, query.params))
          .matches(x -> executed);
    }

    private void executed() {
      executed = true;
    }

    private void prepared(String cql) {
      Assertions.assertThat(cql).isNotNull();
      actualCql = cql;
      expectedExecutions.add(this);
    }
  }
}
