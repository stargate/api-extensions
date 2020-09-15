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

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import io.stargate.db.datastore.Row;
import io.stargate.db.datastore.schema.AbstractTable;
import io.stargate.db.datastore.schema.Column;
import io.stargate.db.datastore.schema.Table;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A simple DseRow implementation that simply stores data in a {@link Map}. Created via {@link
 * ListBackedResultSet}.
 */
public class MapBackedRow implements Row {
  private final Table table;
  private final Map<String, Object> dataMap;

  public static Row of(Table table, Map<String, Object> dataMap) {
    return new MapBackedRow(table, dataMap);
  }

  private MapBackedRow(Table table, Map<String, Object> data) {
    this.table = table;
    this.dataMap = data;
  }

  private <T> T valueFor(Column column) {
    verifyColumn(column);
    //noinspection unchecked
    return (T) dataMap.get(column.name());
  }

  @Override
  public boolean has(String column) {
    return dataMap.containsKey(column);
  }

  @Override
  public List<Column> columns() {
    throw new UnsupportedOperationException(
        "Cannot return columns with simple MapBackedRow implementation.");
  }

  @Override
  public int getInt(Column column) {
    return valueFor(column);
  }

  @Override
  public long getLong(Column column) {
    return valueFor(column);
  }

  @Override
  public long getCounter(Column column) {
    return valueFor(column);
  }

  @Override
  public BigDecimal getDecimal(Column column) {
    return valueFor(column);
  }

  @Override
  public String getString(Column column) {
    return valueFor(column);
  }

  @Override
  public boolean getBoolean(Column column) {
    return valueFor(column);
  }

  @Override
  public byte getByte(Column column) {
    return valueFor(column);
  }

  @Override
  public short getShort(Column column) {
    return valueFor(column);
  }

  @Override
  public double getDouble(Column column) {
    return valueFor(column);
  }

  @Override
  public float getFloat(Column column) {
    return valueFor(column);
  }

  @Override
  public ByteBuffer getBytes(Column column) {
    return valueFor(column);
  }

  @Override
  public InetAddress getInetAddress(Column column) {
    return valueFor(column);
  }

  @Override
  public UUID getUUID(Column column) {
    return valueFor(column);
  }

  @Override
  public BigInteger getVarint(Column column) {
    return valueFor(column);
  }

  @Override
  public Instant getTimestamp(Column column) {
    return valueFor(column);
  }

  @Override
  public LocalTime getTime(Column column) {
    return valueFor(column);
  }

  @Override
  public LocalDate getDate(Column column) {
    return valueFor(column);
  }

  @Override
  public CqlDuration getDuration(Column column) {
    return valueFor(column);
  }

  @Override
  public <T> List<T> getList(Column column) {
    return valueFor(column);
  }

  @Override
  public <T> Set<T> getSet(Column column) {
    return valueFor(column);
  }

  @Override
  public <K, V> Map<K, V> getMap(Column column) {
    return valueFor(column);
  }

  @Override
  public TupleValue getTuple(Column column) {
    verifyColumn(column);
    return valueFor(column);
  }

  @Override
  public UdtValue getUDT(Column column) {
    verifyColumn(column);
    return valueFor(column);
  }

  @Override
  public AbstractTable table() {
    return table;
  }
}
