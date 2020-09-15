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

import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.schema.Column;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.Types;

public class StatementExecutor {

  public static final Method SELECT =
      Types.lookupMethod(StatementExecutor.class, "bindSelect", DataContext.class, String.class);
  public static final Method MODIFY =
      Types.lookupMethod(
          StatementExecutor.class,
          "bindMutation",
          DataContext.class,
          String.class,
          Enumerable.class);

  @SuppressWarnings({"unused", "RedundantSuppression"}) // called from code generated by Calcite
  public static Enumerable<Object> bindSelect(DataContext dataContext, String statementId) {
    RuntimeContext context = (RuntimeContext) dataContext;

    DataSourceStatement statement = context.statement(statementId);

    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        return execute(statement);
      }
    };
  }

  @SuppressWarnings({"unused", "RedundantSuppression"}) // called from code generated by Calcite
  public static Enumerable<Object> bindMutation(
      DataContext dataContext, String statementId, Enumerable<Object[]> params) {
    RuntimeContext context = (RuntimeContext) dataContext;

    DataSourceStatement statement = context.statement(statementId);

    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        return executeMutation(statement, params.enumerator());
      }
    };
  }

  public static Object[] wrap(Object o) {
    return o instanceof Object[] ? (Object[]) o : new Object[] {o};
  }

  private static Enumerator<Object> execute(DataSourceStatement statement) {
    List<Column> columns = statement.resultColumns();

    CompletableFuture<ResultSet> futureResult = statement.execute();

    return new ResultSetEnumerator(columns, futureResult);
  }

  private static Enumerator<Object> executeMutation(
      DataSourceStatement statement, Enumerator<Object[]> params) {
    int count = 0;
    while (params.moveNext()) {
      Object[] values = wrap(params.current());

      CompletableFuture<ResultSet> future = statement.execute(values);
      ResultSet rs = future.join(); // TODO: async upsert statement submission
      count += rs.getExecutionInfo().count();
    }

    return Linq4j.enumerator(Collections.singletonList(count));
  }
}
