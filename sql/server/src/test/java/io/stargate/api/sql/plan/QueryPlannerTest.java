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
package io.stargate.api.sql.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;

import io.stargate.api.sql.AbstractDataStoreTest;
import io.stargate.api.sql.plan.exec.StatementExecutor;
import io.stargate.db.datastore.schema.Column;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.cassandra.stargate.utils.Streams;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class QueryPlannerTest extends AbstractDataStoreTest {

  private final QueryPlanner executor = new QueryPlanner();

  private List<Object[]> execute(PreparedSqlQuery prepared) {
    return Streams.of(prepared.execute(Collections.emptyList()))
        .map(StatementExecutor::wrap)
        .collect(Collectors.toList());
  }

  private PreparedSqlQuery prepare() throws Exception {
    return executor.prepare("SELECT x as z, y from test2", dataStore, "test_ks");
  }

  private PreparedSqlQuery prepareJoin() throws Exception {
    return executor.prepare("SELECT a, x, y from test1, test2 ORDER BY x, a", dataStore, "test_ks");
  }

  @Test
  public void simplePrepare() throws Exception {
    assertThat(prepare().explain()).isEqualTo("CassandraFullScan(table=[[test_ks, test2]])");
  }

  @Test
  public void resultSetMetadata() throws Exception {
    List<Column> columns = prepare().resultSetColumns();
    assertThat(columns).isNotNull();
    assertThat(columns).extracting(Column::name).containsExactly("z", "y");
    assertThat(columns).extracting(Column::type).containsExactly(Column.Type.Int, Column.Type.Text);
  }

  @Test
  public void simpleSelect() throws Exception {
    PreparedSqlQuery prepared = prepare();
    List<Object[]> result = execute(prepared);
    assertThat(result).extracting(a -> a[0]).containsExactly(1, 2);
    assertThat(result).extracting(a -> a[1]).containsExactly("row_1", "row_2");
  }

  @Test
  public void simpleInsert() throws Exception {
    PreparedSqlQuery prepared =
        executor.prepare("INSERT INTO test2 (x, y) VALUES (11, 'aaa')", dataStore, "test_ks");
    Mockito.verify(dataStore, times(1))
        .prepare(Mockito.eq("INSERT INTO test_ks.test2 (x, y) VALUES (?, ?)"), any());
    List<Object[]> result = execute(prepared);
    Mockito.verify(dataStore, times(1))
        .query("INSERT INTO test_ks.test2 (x, y) VALUES (?, ?)", 11, "aaa");
    assertThat(result).extracting(a -> a[0]).containsExactly(1);
  }

  @Test
  public void simpleUpdate() throws Exception {
    PreparedSqlQuery prepared =
        executor.prepare("UPDATE test2 SET y = 'bbb' WHERE x = 1", dataStore, "test_ks");
    Mockito.verify(dataStore, times(1))
        .prepare(Mockito.eq("INSERT INTO test_ks.test2 (x, y) VALUES (?, ?)"), any());
    Mockito.verify(dataStore, times(1))
        .prepare(Mockito.eq("SELECT x, y FROM test_ks.test2"), any());
    Mockito.verify(dataStore, times(2))
        .prepare(Mockito.anyString(), any()); // No other statements were prepared

    List<Object[]> result = execute(prepared);
    Mockito.verify(dataStore, times(1))
        .query("INSERT INTO test_ks.test2 (x, y) VALUES (?, ?)", 1, "bbb");
    assertThat(result).extracting(a -> a[0]).containsExactly(1);
  }

  @Test
  public void simpleDelete() throws Exception {
    PreparedSqlQuery prepared =
        executor.prepare("DELETE FROM test2 WHERE x = 2", dataStore, "test_ks");
    Mockito.verify(dataStore, times(1))
        .prepare(Mockito.eq("DELETE FROM test_ks.test2 WHERE x = ?"), any());
    Mockito.verify(dataStore, times(1))
        .prepare(Mockito.eq("SELECT x, y FROM test_ks.test2"), any());
    Mockito.verify(dataStore, times(2))
        .prepare(Mockito.anyString(), any()); // No other statements were prepared

    List<Object[]> result = execute(prepared);
    Mockito.verify(dataStore, times(1)).query("DELETE FROM test_ks.test2 WHERE x = ?", 2);
    assertThat(result).extracting(a -> a[0]).containsExactly(1);
  }

  @Test
  public void joinSingleColumnToMultiColumnTable() throws Exception {
    PreparedSqlQuery prepared = prepareJoin();
    List<Object[]> result = execute(prepared);
    assertThat(result).extracting(a -> a[0]).containsExactly(10, 20, 10, 20);
    assertThat(result).extracting(a -> a[1]).containsExactly(1, 1, 2, 2);
    assertThat(result).extracting(a -> a[2]).containsExactly("row_1", "row_1", "row_2", "row_2");
  }
}
