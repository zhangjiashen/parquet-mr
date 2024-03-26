/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.impl;

import org.apache.parquet.column.BufferedColumnWriter;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.io.api.Binary;

import java.util.ArrayList;
import java.util.List;

/**
 * NotifyColumnWriter notifies any registered BufferedColumnWriters with values written to it.
 * Currently, only supports Binary type
 */
public class NotifyColumnWriter implements ColumnWriter {
  private static final String UNSUPPORTED_OP_ERR_MSG = "Non-Binary type columns not supported" +
    " by NotifyColumnWriter";
  private final ColumnWriter columnWriter;
  private final List<BufferedColumnWriter> bufferedColumnObservers;

  public NotifyColumnWriter(ColumnWriter columnWriter) {
    this.columnWriter = columnWriter;
    bufferedColumnObservers = new ArrayList<>();
  }

  @Override
  public void write(int value, int repetitionLevel, int definitionLevel) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_ERR_MSG);
  }

  @Override
  public void write(long value, int repetitionLevel, int definitionLevel) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_ERR_MSG);
  }

  @Override
  public void write(boolean value, int repetitionLevel, int definitionLevel) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_ERR_MSG);
  }

  @Override
  public void write(Binary value, int repetitionLevel, int definitionLevel) {
    notifyObservers(value, repetitionLevel, definitionLevel);
    columnWriter.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(float value, int repetitionLevel, int definitionLevel) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_ERR_MSG);
  }

  @Override
  public void write(double value, int repetitionLevel, int definitionLevel) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_ERR_MSG);
  }

  @Override
  public void writeNull(int repetitionLevel, int definitionLevel) {
    notifyObservers(null, repetitionLevel, definitionLevel);
    columnWriter.writeNull(repetitionLevel, definitionLevel);
  }

  @Override
  public void close() {
    columnWriter.close();
  }

  @Override
  public long getBufferedSizeInMemory() {
    return columnWriter.getBufferedSizeInMemory();
  }

  private void notifyObservers(Binary value, int repetitionLevel, int definitionLevel) {
    for (BufferedColumnWriter bufferedColumnWriter : bufferedColumnObservers) {
      bufferedColumnWriter.receiveValue(value, repetitionLevel, definitionLevel);
    }
  }

  public void addObserver(BufferedColumnWriter bufferedColumnWriter) {
    bufferedColumnObservers.add(bufferedColumnWriter);
  }
}
