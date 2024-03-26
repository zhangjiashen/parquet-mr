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

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.io.api.Binary;

/**
 * writer for (repetition level, definition level, values) triplets
 */
public interface ColumnWriter {

  /**
   * writes the current value
   * @param value an int value
   * @param repetitionLevel a repetition level
   * @param definitionLevel a definition level
   */
  void write(int value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current value
   * @param value a long value
   * @param repetitionLevel a repetition level
   * @param definitionLevel a definition level
   */
  void write(long value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current value
   * @param value a boolean value
   * @param repetitionLevel a repetition level
   * @param definitionLevel a definition level
   */
  void write(boolean value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current value
   * @param value a Binary value
   * @param repetitionLevel a repetition level
   * @param definitionLevel a definition level
   */
  void write(Binary value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current value
   * @param value a float value
   * @param repetitionLevel a repetition level
   * @param definitionLevel a definition level
   */
  void write(float value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current value
   * @param value a double value
   * @param repetitionLevel a repetition level
   * @param definitionLevel a definition level
   */
  void write(double value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current null value
   * @param repetitionLevel a repetition level
   * @param definitionLevel a definition level
   */
  void writeNull(int repetitionLevel, int definitionLevel);

  /**
   * Writes the current null value but uses valueForStats to update the stats instead of incrementing null count.
   * This is used for RootComply Cell Encryption where the original column must track
   * the stats of values written to the encr sister column.
   * @param repetitionLevel a repetition level
   * @param definitionLevel a definition level
   * @param valueForStats a value written to the sister encr column which
   *                      should be used for statistics purposes in current column.
   */
  default void writeNullWithValueForStats(int repetitionLevel, int definitionLevel, int valueForStats) {
    throw new UnsupportedOperationException("writeNullWithValueForStats not implemented for this ColumnWriter");
  }

  default void writeNullWithValueForStats(int repetitionLevel, int definitionLevel, long valueForStats) {
    throw new UnsupportedOperationException("writeNullWithValueForStats not implemented for this ColumnWriter");
  }

  default void writeNullWithValueForStats(int repetitionLevel, int definitionLevel, boolean valueForStats) {
    throw new UnsupportedOperationException("writeNullWithValueForStats not implemented for this ColumnWriter");
  }

  default void writeNullWithValueForStats(int repetitionLevel, int definitionLevel, Binary valueForStats) {
    throw new UnsupportedOperationException("writeNullWithValueForStats not implemented for this ColumnWriter");
  }

  default void writeNullWithValueForStats(int repetitionLevel, int definitionLevel, float valueForStats) {
    throw new UnsupportedOperationException("writeNullWithValueForStats not implemented for this ColumnWriter");
  }

  default void writeNullWithValueForStats(int repetitionLevel, int definitionLevel, double valueForStats) {
    throw new UnsupportedOperationException("writeNullWithValueForStats not implemented for this ColumnWriter");
  }

  /**
  * Close the underlying store. This should be called when there are no
  * more data to be written.
  */
  void close();

  /**
   * used to decide when to write a page or row group
   * @return the number of bytes of memory used to buffer the current data
   */
  long getBufferedSizeInMemory();
}

