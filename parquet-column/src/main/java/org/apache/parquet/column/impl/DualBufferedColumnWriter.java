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
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public class DualBufferedColumnWriter implements BufferedColumnWriter {
  private final ColumnWriter originalColumnWriter;
  private final Binding bindedColWriter;
  private final ColumnWriter encryptColumnWriter;
  private final EncryptBinding bindedCellManager;
  private final CellValueBuffer cellValueBuffer;
  private final CellValueBuffer<Binary> providerValues;
  private final int definitionLevelToWriteNull;
  private final int maxProviderRepetitionLevel;
  private CellValueBuffer.CellValue<Binary> currentProviderCell;

  public DualBufferedColumnWriter(ColumnWriter originColumnWriter, ColumnWriter encryptColumnWriter,
                                  CellManager cellManager, PrimitiveTypeName bufferType,
                                  int definitionLevelToWriteNull, int maxProviderRepetitionLevel) {
    this.originalColumnWriter = originColumnWriter;
    this.bindedColWriter = bind(originColumnWriter, bufferType);
    this.encryptColumnWriter = encryptColumnWriter;
    this.bindedCellManager = bindCellManager(cellManager, bufferType);
    this.cellValueBuffer = new CellValueBuffer<>();
    this.providerValues = new CellValueBuffer<>();
    // Store parent definition level, so we can writeNull during dualWrite.
    this.definitionLevelToWriteNull = definitionLevelToWriteNull;
    this.maxProviderRepetitionLevel = maxProviderRepetitionLevel;
    this.currentProviderCell = null;
  }

  @Override
  public void write(int value, int repetitionLevel, int definitionLevel) {
    cellValueBuffer.addValue(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(long value, int repetitionLevel, int definitionLevel) {
    cellValueBuffer.addValue(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(boolean value, int repetitionLevel, int definitionLevel) {
    cellValueBuffer.addValue(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(Binary value, int repetitionLevel, int definitionLevel) {
    cellValueBuffer.addValue(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(float value, int repetitionLevel, int definitionLevel) {
    cellValueBuffer.addValue(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(double value, int repetitionLevel, int definitionLevel) {
    cellValueBuffer.addValue(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void writeNull(int repetitionLevel, int definitionLevel) {
    // writeNull may be called at writer .close() -> .flush() time,
    // so we must also flush this DualBufferedColumnWriter at close() time
    // (in addition to endMessage time) to ensure the null is actually written.
    cellValueBuffer.addValue(null, repetitionLevel, definitionLevel);
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
  public void receiveValue(Binary providerValue, int repetitionLevel, int definitionLevel) {
    providerValues.addValue(providerValue, repetitionLevel, definitionLevel);
  }

  @Override
  public void validateBuffersEmpty() {
    if (cellValueBuffer.hasNext() || providerValues.hasNext()) {
      throw new IllegalStateException("DualBufferedColumnWriter value or provider buffers are not empty." +
        " Expected to be empty after closing writer");
    }
  }

  @Override
  public void writeBufferedValues(boolean isFinalFlush) {
    while(cellValueBuffer.hasNext()) {
      if (cellValueBuffer.peek().getRepLevel() <= maxProviderRepetitionLevel) {
        if (providerValues.hasNext()) {
          currentProviderCell = providerValues.next();
        } else {
          // Need to move to next provider, but it's not buffered yet.
          // Break and wait for more provider values to be buffered.
          break;
        }
      }
      CellValueBuffer.CellValue cellValue = cellValueBuffer.next();
      if (currentProviderCell == null || cellValue.getRepLevel() < currentProviderCell.getRepLevel()) {
        throw new IllegalStateException("currentProviderCell not set or set with unexpected repetition level");
      }
      if (cellValue.getValue() == null) {
        dualNull(cellValue.getRepLevel(), cellValue.getDefLevel());
      } else {
        String provider = currentProviderCell.getValue() == null ?
          null : currentProviderCell.getValue().toStringUsingUTF8();
        dualWrite(cellValue, provider);
      }
    }
    if (isFinalFlush) {
      // Both Provider and Original Buffers should be emptied during the final flush.
      this.validateBuffersEmpty();
    }
  }

  private void dualWrite(CellValueBuffer.CellValue<?> cellValue, String provider) {
    byte[] encryptedValue = bindedCellManager.encrypt(cellValue, provider);
    boolean shouldEncrypt = encryptedValue != null;
    int repetitionLevel = cellValue.getRepLevel();
    int definitionLevel = cellValue.getDefLevel();

    if (shouldEncrypt) {
      bindedColWriter.writeNullWithValueForStats(repetitionLevel, definitionLevelToWriteNull, cellValue.getValue());
      Binary binaryVal = Binary.fromConstantByteArray(encryptedValue);
      encryptColumnWriter.write(binaryVal, repetitionLevel, definitionLevel);
    } else {
      bindedColWriter.write(cellValue);
      encryptColumnWriter.writeNull(repetitionLevel, definitionLevelToWriteNull);
    }
  }

  private void dualNull(int repetitionLevel, int definitionLevel) {
    originalColumnWriter.writeNull(repetitionLevel, definitionLevel);
    encryptColumnWriter.writeNull(repetitionLevel, definitionLevel);
  }

  interface EncryptBinding {
    byte[] encrypt(CellValueBuffer.CellValue cellValue, String provider);
  }

  EncryptBinding bindCellManager(CellManager cellManager, PrimitiveTypeName type) {
    switch(type) {
      case INT32:
        return (cellValue, provider) -> cellManager.encryptInt((int)cellValue.getValue(), provider);
      case INT64:
        return (cellValue, provider) -> cellManager.encryptLong((long)cellValue.getValue(), provider);
      case BOOLEAN:
        return (cellValue, provider) -> cellManager.encryptBoolean((boolean)cellValue.getValue(), provider);
      case BINARY:
      case INT96:
      case FIXED_LEN_BYTE_ARRAY:
        return (cellValue, provider) -> cellManager.encryptBinary((Binary)cellValue.getValue(), provider);
      case FLOAT:
        return (cellValue, provider) -> cellManager.encryptFloat((float)cellValue.getValue(), provider);
      case DOUBLE:
        return (cellValue, provider) -> cellManager.encryptDouble((double)cellValue.getValue(), provider);
      default:
        throw new IllegalStateException("Type is invalid for writing buffered values," +
          " type = " + type);
    }
  }
}
