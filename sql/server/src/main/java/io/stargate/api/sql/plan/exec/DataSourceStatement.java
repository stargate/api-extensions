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
package io.stargate.api.sql.plan.exec;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.stargate.api.sql.schema.StargateTable;
import io.stargate.api.sql.schema.TypeUtils;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.query.*;
import io.stargate.db.datastore.schema.Column;
import io.stargate.db.datastore.schema.Table;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public class DataSourceStatement {
  public static final ConsistencyLevel DEFAULT_CL = ConsistencyLevel.LOCAL_QUORUM;

  private final DataStore dataStore;
  private final MixinPreparedStatement<Object[]> statement;
  private final List<Column> resultColumns;

  private DataSourceStatement(
      DataStore dataStore, MixinPreparedStatement<?> prepared, List<Column> resultColumns) {
    this.dataStore = dataStore;
    //noinspection unchecked
    this.statement = (MixinPreparedStatement<Object[]>) prepared;
    this.resultColumns = resultColumns;
  }

  public List<Column> resultColumns() {
    return resultColumns;
  }

  public CompletableFuture<ResultSet> execute(Object... params) {
    return statement.execute(dataStore, params);
  }

  private static Object bind(Column.ColumnType type, int idx, Object[] params) {
    Object value = params[idx];
    return TypeUtils.toDriverValue(value, type);
  }

  public static DataSourceStatement fullScan(DataStore dataStore, StargateTable sqlTable) {
    Table table = sqlTable.table();
    List<Column> columns = table.columns();

    MixinPreparedStatement<?> prepared =
        dataStore
            .query()
            .select()
            .column(columns)
            .from(sqlTable.keyspace(), table)
            .consistencyLevel(DEFAULT_CL)
            .prepare();

    return new DataSourceStatement(dataStore, prepared, columns);
  }

  public static DataSourceStatement upsert(
      DataStore dataStore, StargateTable sqlTable, List<Column> columns) {
    Table table = sqlTable.table();

    ImmutableList.Builder<Value<?>> list = ImmutableList.builder();
    int idx = 0;
    for (Column c : columns) {
      int i = idx++;
      Column.ColumnType type = Objects.requireNonNull(c.type());
      list.add(
          ImmutableValue.<Object[]>builder()
              .column(c)
              .bindingFunction(p -> bind(type, i, p))
              .build());
    }
    List<Value<?>> values = list.build();

    MixinPreparedStatement<?> prepared =
        dataStore
            .query()
            .insertInto(sqlTable.keyspace(), table)
            .value(values)
            .consistencyLevel(DEFAULT_CL)
            .prepare();

    return new DataSourceStatement(dataStore, prepared, Collections.emptyList());
  }

  public static DataSourceStatement delete(
      DataStore dataStore, StargateTable sqlTable, List<Column> keyColumns) {
    Table table = sqlTable.table();

    ImmutableList.Builder<Where<?>> list = ImmutableList.builder();
    int idx = 0;
    for (Column c : keyColumns) {
      int i = idx++;
      Column.ColumnType type = Objects.requireNonNull(c.type());
      list.add(
          ImmutableWhereCondition.<Object[]>builder()
              .column(c)
              .bindingFunction(p -> bind(type, i, p))
              .predicate(WhereCondition.Predicate.Eq)
              .build());
    }
    ImmutableList<Where<?>> wheres = list.build();

    MixinPreparedStatement<?> prepared =
        dataStore
            .query()
            .delete()
            .from(sqlTable.keyspace(), table)
            .where(wheres)
            .consistencyLevel(DEFAULT_CL)
            .prepare();

    return new DataSourceStatement(dataStore, prepared, Collections.emptyList());
  }
}
