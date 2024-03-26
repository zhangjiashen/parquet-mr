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
package org.apache.parquet.column;

import org.apache.parquet.io.CellValueBuffer;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

public interface BufferedColumnWriter extends ColumnWriter {

  /**
   * Receive value triplet from another column.
   * This method allows matching values between this column
   * and another column within the record.
   * @param value Binary
   * @param repetitionLevel int
   * @param definitionLevel int
   */
  void receiveValue(Binary value, int repetitionLevel, int definitionLevel);

  /**
   * Verify that all buffered values have been flushed
   * to the underlying column writers
   */
  void validateBuffersEmpty();

  /**
   * writeBufferedValues called once per message to flush
   * any buffered values to column writers.
   * @param isFinalFlush indicates if ParquetWriter is being closed.
   */
  void writeBufferedValues(boolean isFinalFlush);

  interface Binding {
    void write(CellValueBuffer.CellValue cellValue);
    void writeNullWithValueForStats(int repetitionLevel, int definitionLevel, Object valueForStats);
  }

  default Binding bind(ColumnWriter cw, PrimitiveType.PrimitiveTypeName type) {
    switch(type) {
      case INT32:
        return new Binding() {
          @Override
          public void write(CellValueBuffer.CellValue cellValue) {
            cw.write((int)cellValue.getValue(), cellValue.getRepLevel(), cellValue.getDefLevel());
          }

          @Override
          public void writeNullWithValueForStats(int repetitionLevel, int definitionLevel, Object valueForStats) {
            cw.writeNullWithValueForStats(repetitionLevel, definitionLevel, (int)valueForStats);
          }
        };
      case INT64:
        return new Binding() {
          @Override
          public void write(CellValueBuffer.CellValue cellValue) {
            cw.write((long)cellValue.getValue(), cellValue.getRepLevel(), cellValue.getDefLevel());
          }

          @Override
          public void writeNullWithValueForStats(int repetitionLevel, int definitionLevel, Object valueForStats) {
            cw.writeNullWithValueForStats(repetitionLevel, definitionLevel, (long)valueForStats);
          }
        };
      case BOOLEAN:
        return new Binding() {
          @Override
          public void write(CellValueBuffer.CellValue cellValue) {
            cw.write((boolean)cellValue.getValue(), cellValue.getRepLevel(), cellValue.getDefLevel());
          }

          @Override
          public void writeNullWithValueForStats(int repetitionLevel, int definitionLevel, Object valueForStats) {
            cw.writeNullWithValueForStats(repetitionLevel, definitionLevel, (boolean)valueForStats);
          }
        };
      case BINARY:
      case INT96:
      case FIXED_LEN_BYTE_ARRAY:
        return new Binding() {
          @Override
          public void write(CellValueBuffer.CellValue cellValue) {
            cw.write((Binary)cellValue.getValue(), cellValue.getRepLevel(), cellValue.getDefLevel());
          }

          @Override
          public void writeNullWithValueForStats(int repetitionLevel, int definitionLevel, Object valueForStats) {
            cw.writeNullWithValueForStats(repetitionLevel, definitionLevel, (Binary)valueForStats);
          }
        };
      case FLOAT:
        return new Binding() {
          @Override
          public void write(CellValueBuffer.CellValue cellValue) {
            cw.write((float) cellValue.getValue(), cellValue.getRepLevel(), cellValue.getDefLevel());
          }

          @Override
          public void writeNullWithValueForStats(int repetitionLevel, int definitionLevel, Object valueForStats) {
            cw.writeNullWithValueForStats(repetitionLevel, definitionLevel, (float)valueForStats);
          }
        };
      case DOUBLE:
        return new Binding() {
          @Override
          public void write(CellValueBuffer.CellValue cellValue) {
            cw.write((double)cellValue.getValue(), cellValue.getRepLevel(), cellValue.getDefLevel());
          }

          @Override
          public void writeNullWithValueForStats(int repetitionLevel, int definitionLevel, Object valueForStats) {
            cw.writeNullWithValueForStats(repetitionLevel, definitionLevel, (double)valueForStats);
          }
        };
      default:
        throw new IllegalStateException("Type is invalid for writing cell encrypted values," +
          " type = " + type);
    }
  }
}
