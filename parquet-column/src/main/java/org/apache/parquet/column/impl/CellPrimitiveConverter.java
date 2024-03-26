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

import org.apache.parquet.column.CellManager;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeNameConverter;

/**
 * CellPrimitiveConverter receives encrypted column value, decrypts it and pipes
 * to original column primitive converter.
 */
public class CellPrimitiveConverter extends PrimitiveConverter {
  private final Binding binding;
  private Dictionary dictionary;

  /**
   * Creates a CellPrimitiveConverter to decrypt and pipe values to an original
   * column value converter.
   *
   * @param cellManager Cell encryption manager for decryption.
   * @param originType The original column value type.
   * @param originConverter The original column value converter.
   */
  public CellPrimitiveConverter(CellManager cellManager, PrimitiveType originType, PrimitiveConverter originConverter) {
    this.binding = bind(cellManager, originType, originConverter);
  }

  /**
   * Creates a dummy converter that discards values. Used when only querying
   * cell encrypted column. Not requesting original column to receive results.
   */
  public CellPrimitiveConverter() {
    this.binding = new DiscardBinding();
  }

  @Override
  public boolean hasDictionarySupport() {
    return true;
  }

  @Override
  public void setDictionary(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  @Override
  public void addValueFromDictionary(int dictionaryId) {
    Binary value = dictionary.decodeToBinary(dictionaryId);
    addBinary(value);
  }

  @Override
  public void addBinary(Binary value) {
    try {
      binding.addValue(value.getBytesUnsafe());
    } catch (CellManager.KeyNotFoundException e) {
      // Do nothing, key deleted, keep original column as NULL
    }
  }

  private static Binding bind(CellManager cellManager, PrimitiveType type, PrimitiveConverter converter) {
    return type.getPrimitiveTypeName().convert(new PrimitiveTypeNameConverter<Binding, RuntimeException>() {
      @Override
      public Binding convertFLOAT(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return value -> converter.addFloat(cellManager.decryptFloat(value));
      }
      @Override
      public Binding convertDOUBLE(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return value -> converter.addDouble(cellManager.decryptDouble(value));
      }
      @Override
      public Binding convertINT32(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return value -> converter.addInt(cellManager.decryptInt(value));
      }
      @Override
      public Binding convertINT64(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return value -> converter.addLong(cellManager.decryptLong(value));
      }
      @Override
      public Binding convertINT96(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return this.convertBINARY(primitiveTypeName);
      }
      @Override
      public Binding convertFIXED_LEN_BYTE_ARRAY(
        PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return this.convertBINARY(primitiveTypeName);
      }
      @Override
      public Binding convertBOOLEAN(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return value -> converter.addBoolean(cellManager.decryptBoolean(value));
      }
      @Override
      public Binding convertBINARY(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return value -> converter.addBinary(cellManager.decryptBinary(value));
      }
    });
  }

  private interface Binding {
    void addValue(byte[] value);
  }

  private static class DiscardBinding implements Binding {
    @Override
    public void addValue(byte[] value) {
      // do nothing
    }
  }

}
