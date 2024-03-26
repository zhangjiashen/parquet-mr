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
import org.apache.parquet.column.CellManager;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.io.CellValueBuffer;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

public class StatsBufferedColumnWriter implements BufferedColumnWriter {
  private final ColumnWriter originalColumnWriter;
  private final Binding bindedColWriter;
  private final DecryptBinding bindedCellManager;
  private final CellValueBuffer originalValueBuffer;
  private final CellValueBuffer<Binary> statsValueBuffer;
  private final String columnName;
  private final String encryptedColumnName;

  public StatsBufferedColumnWriter(ColumnWriter originalColumnWriter,
                                   CellManager cellManager, PrimitiveType.PrimitiveTypeName writerType,
                                   String columnName, String encryptedColumnName) {
    this.originalColumnWriter = originalColumnWriter;
    this.bindedColWriter = bind(originalColumnWriter, writerType);
    this.bindedCellManager = bindDecrypt(cellManager, writerType);
    this.originalValueBuffer = new CellValueBuffer<>();
    this.statsValueBuffer = new CellValueBuffer<>();
    this.columnName = columnName;
    this.encryptedColumnName = encryptedColumnName;
  }

  @Override
  public void write(int value, int repetitionLevel, int definitionLevel) {
    originalValueBuffer.addValue(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(long value, int repetitionLevel, int definitionLevel) {
    originalValueBuffer.addValue(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(boolean value, int repetitionLevel, int definitionLevel) {
    originalValueBuffer.addValue(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(Binary value, int repetitionLevel, int definitionLevel) {
    originalValueBuffer.addValue(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(float value, int repetitionLevel, int definitionLevel) {
    originalValueBuffer.addValue(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(double value, int repetitionLevel, int definitionLevel) {
    originalValueBuffer.addValue(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void writeNull(int repetitionLevel, int definitionLevel) {
    originalValueBuffer.addValue(null, repetitionLevel, definitionLevel);
  }

  @Override
  public void close() {
    originalColumnWriter.close();
  }

  @Override
  public long getBufferedSizeInMemory() {
    return originalColumnWriter.getBufferedSizeInMemory();
  }

  @Override
  public void receiveValue(Binary encryptedValue, int repetitionLevel, int definitionLevel) {
    statsValueBuffer.addValue(encryptedValue, repetitionLevel, definitionLevel);
  }

  @Override
  public void validateBuffersEmpty() {
    if (originalValueBuffer.hasNext()) {
      throw new IllegalStateException("StatsBufferedColumnWriter: originalValueBuffer is not empty." +
        " Expected to be empty after closing writer");
    } else if (statsValueBuffer.hasNext()) {
      throw new IllegalStateException("StatsBufferedColumnWriter: statsValueBuffer is not empty." +
        " Expected to be empty after closing writer");
    }
  }

  @Override
  public void writeBufferedValues(boolean isFinalFlush) {
    while(originalValueBuffer.hasNext() && statsValueBuffer.hasNext()) {
      CellValueBuffer.CellValue origValue = originalValueBuffer.next();
      CellValueBuffer.CellValue encrValue = statsValueBuffer.next();

      if (origValue.getValue() == null && encrValue.getValue() == null) {
        originalColumnWriter.writeNull(origValue.getRepLevel(), origValue.getDefLevel());
      } else if (origValue.getValue() == null && encrValue.getValue() != null) {
        byte[] encrBytes = ((Binary)encrValue.getValue()).getBytes();
        this.bindedColWriter.writeNullWithValueForStats(origValue.getRepLevel(),
          origValue.getDefLevel(), this.bindedCellManager.decrypt(encrBytes));
      } else if (origValue.getValue() != null && encrValue.getValue() == null) {
        this.bindedColWriter.write(origValue);
      } else {
        // Both orig / encrValue hold a value => illegal state
        throw new IllegalStateException("Both original column value and corresponding encrypted" +
          " column value have a non-null value simultaneously:" +
          " original = [" + columnName + "] and encrypted = [" + encryptedColumnName + "]");
      }
    }
    if (isFinalFlush) {
      this.validateBuffersEmpty();
    }
  }

  interface DecryptBinding {
    Object decrypt(byte[] encrBytes);
  }

  DecryptBinding bindDecrypt(CellManager cellManager, PrimitiveType.PrimitiveTypeName type) {
    switch(type) {
      case INT32:
        return cellManager::decryptInt;
      case INT64:
        return cellManager::decryptLong;
      case BOOLEAN:
        return cellManager::decryptBoolean;
      case BINARY:
      case INT96:
      case FIXED_LEN_BYTE_ARRAY:
        return cellManager::decryptBinary;
      case FLOAT:
        return cellManager::decryptFloat;
      case DOUBLE:
        return cellManager::decryptDouble;
      default:
        throw new IllegalStateException("Type is invalid for StatsBufferedColumnWriter values," +
          " type = " + type);
    }
  }
}
