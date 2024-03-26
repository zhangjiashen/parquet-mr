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
import org.apache.parquet.schema.Type.Repetition;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestCellPrimitiveConverter {

  @Test
  public void testConvert() {
    testConvert(PrimitiveTypeName.FLOAT, 1.2f, CellManager::decryptFloat, PrimitiveConverter::addFloat);
    testConvert(PrimitiveTypeName.DOUBLE, 1.2, CellManager::decryptDouble, PrimitiveConverter::addDouble);
    testConvert(PrimitiveTypeName.INT32, 123, CellManager::decryptInt, PrimitiveConverter::addInt);
    testConvert(PrimitiveTypeName.INT64, 123l, CellManager::decryptLong, PrimitiveConverter::addLong);
    testConvert(PrimitiveTypeName.BOOLEAN, true, CellManager::decryptBoolean, PrimitiveConverter::addBoolean);
    testConvert(PrimitiveTypeName.INT96, Binary.fromCharSequence("abc"), CellManager::decryptBinary,
      PrimitiveConverter::addBinary);
    testConvert(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Binary.fromCharSequence("def"), CellManager::decryptBinary,
      PrimitiveConverter::addBinary);
    testConvert(PrimitiveTypeName.BINARY, Binary.fromCharSequence("ghi"), CellManager::decryptBinary,
      PrimitiveConverter::addBinary);
  }

  <T> void testConvert(PrimitiveTypeName typeName, T value, Decrypt<T> decrypt, Verify<T> verifier) {
    CellManager cellManager = mock(CellManager.class);
    PrimitiveType originType = new PrimitiveType(Repetition.OPTIONAL, typeName, "");
    PrimitiveConverter originConverter = mock(PrimitiveConverter.class);
    byte[] bytes = new byte[] { 1, 2, 3, 4 };
    Binary binary = Binary.fromConstantByteArray(bytes);

    when(decrypt.apply(cellManager, bytes)).thenReturn(value);

    PrimitiveConverter cellConverter = new CellPrimitiveConverter(cellManager, originType, originConverter);
    Assert.assertTrue(cellConverter.hasDictionarySupport());

    // test add value from binary
    cellConverter.addBinary(binary);

    // test add value from dictionary
    Dictionary dict = mock(Dictionary.class);
    when(dict.decodeToBinary(12)).thenReturn(binary);
    cellConverter.setDictionary(dict);
    cellConverter.addValueFromDictionary(12);

    // verify on original converter the right primitive method has been called
    // twice (1 from binary and 1 from dictionary) with the expected value
    verifier.apply(verify(originConverter, times(2)), value);

    // test dummy dicard converter
    PrimitiveConverter discardConverter = new CellPrimitiveConverter();
    discardConverter.addBinary(binary);
    discardConverter.setDictionary(dict);
    discardConverter.addValueFromDictionary(12);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testException() {
    CellManager cellManager = mock(CellManager.class);
    PrimitiveType originType = new PrimitiveType(
      Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "");
    PrimitiveConverter originConverter = mock(PrimitiveConverter.class);
    byte[] bytes = new byte[] { 1, 2, 3, 4 };
    Binary binary = Binary.fromConstantByteArray(bytes);

    when(cellManager.decryptDouble(bytes)).thenThrow(new IllegalArgumentException("test-err"));

    PrimitiveConverter cellConverter = new CellPrimitiveConverter(
      cellManager, originType, originConverter);
    cellConverter.addBinary(binary);
  }

  @Test
  public void testKeyNotFoundException() {
    CellManager cellManager = mock(CellManager.class);
    PrimitiveType originType = new PrimitiveType(
      Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "");
    PrimitiveConverter originConverter = mock(PrimitiveConverter.class);
    byte[] bytes = new byte[] { 1, 2, 3, 4 };
    Binary binary = Binary.fromConstantByteArray(bytes);

    when(cellManager.decryptDouble(bytes)).thenThrow(
      new CellManager.KeyNotFoundException("test-err"));

    PrimitiveConverter cellConverter = new CellPrimitiveConverter(
      cellManager, originType, originConverter);
    cellConverter.addBinary(binary);

    // nothing should have happened to originConverter (original value remain NULL)
    verifyNoMoreInteractions(originConverter);
  }

  static interface Decrypt<T> {
    T apply(CellManager cellManager, byte[] value);
  }

  static interface Verify<T> {
    void apply(PrimitiveConverter verify, T value);
  }
}
