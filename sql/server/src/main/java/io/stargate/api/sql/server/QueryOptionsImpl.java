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

import io.stargate.db.QueryOptions;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.transport.ProtocolVersion;

public class QueryOptionsImpl implements QueryOptions {
  @Override
  public ConsistencyLevel getConsistency() {
    return ConsistencyLevel.LOCAL_QUORUM;
  }

  @Override
  public ConsistencyLevel getSerialConsistency() {
    return ConsistencyLevel.LOCAL_SERIAL;
  }

  @Override
  public List<ByteBuffer> getValues() {
    return Collections.emptyList();
  }

  @Override
  public List<String> getNames() {
    return Collections.emptyList();
  }

  @Override
  public ProtocolVersion getProtocolVersion() {
    return ProtocolVersion.CURRENT;
  }

  @Override
  public int getPageSize() {
    return 100;
  }

  @Override
  public ByteBuffer getPagingState() {
    return null;
  }

  @Override
  public long getTimestamp() {
    return 0; // TODO: query state values
  }

  @Override
  public int getNowInSeconds() {
    return 0; // TODO: query state values
  }

  @Override
  public String getKeyspace() {
    return null;
  }

  @Override
  public boolean skipMetadata() {
    return true;
  }
}
