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
package io.stargate.api.sql.server;

import io.stargate.db.ClientState;
import io.stargate.db.Persistence;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;

public class StargateConnection {

  private final Meta.ConnectionHandle ch;
  private final AtomicInteger statementSeq = new AtomicInteger();
  private final ConcurrentMap<Integer, StargateStatement> statements = new ConcurrentHashMap<>();

  private final ClientState clientState;

  public StargateConnection(Meta.ConnectionHandle ch, ClientState clientState) {
    this.ch = ch;
    this.clientState = clientState;
  }

  private StargateStatement newStatementHandle(Function<Integer, StargateStatement> builder) {
    int id = statementSeq.incrementAndGet();
    if (id >= Integer.MAX_VALUE) {
      throw new IllegalStateException("Too many statements created");
    }

    return statements.computeIfAbsent(id, builder);
  }

  public StargateStatement newStatement(String sql, Persistence persistence) {
    return newStatementHandle(
        id -> {
          QueryState queryState = persistence.newQueryState(clientState);
          DataStore dataStore = persistence.newDataStore(queryState, new QueryOptionsImpl());

          return StargateStatement.prepare(ch.id, id, sql, dataStore);
        });
  }

  public StargateStatement newStatement(Persistence persistence) {
    return newStatementHandle(
        id -> {
          QueryState queryState = persistence.newQueryState(clientState);
          DataStore dataStore = persistence.newDataStore(queryState, new QueryOptionsImpl());

          return StargateStatement.empty(ch.id, id, dataStore);
        });
  }

  public StargateStatement statement(Meta.StatementHandle h) throws NoSuchStatementException {
    StargateStatement statement = statements.get(h.id);

    if (statement == null) {
      throw new NoSuchStatementException(h);
    }

    return statement;
  }

  public void closeStatement(Meta.StatementHandle h) {
    statements.remove(h.id);
  }
}
