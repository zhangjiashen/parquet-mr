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
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Reference unit tests are the CryptoCellManager unit tests in crypto-retriever:
 * https://code.uberinternal.com/D7326619
 */
public class TestBasicCellManager {

  @Test
  public void testEncryptDecryptFloat() {
    CellManager cellManager = new BasicCellManager();
    float delta = 0.000001f;

    for (float value : new float[] {1.11f, -1.11f, Float.MAX_VALUE, Float.MIN_VALUE, 0}) {
      float retValue =
        cellManager.decryptFloat(cellManager.encryptFloat(value, BasicCellManager.BASIC_PROVIDER_VAL));
      assertEquals(value, retValue, delta);
    }
  }

  @Test
  public void testEncryptDecryptDouble() {
    CellManager cellManager = new BasicCellManager();
    float delta = 0.000001f;

    for (double value : new double[]{1.11, -1.11, Double.MAX_VALUE, Double.MIN_VALUE, 0}) {
      double retValue =
        cellManager.decryptDouble(cellManager.encryptDouble(value, BasicCellManager.BASIC_PROVIDER_VAL));
      assertEquals(value, retValue, delta);
    }
  }

  @Test
  public void testEncryptDecryptInt() {
    CellManager cellManager = new BasicCellManager();

    for (int value : new int[] {1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, 0}) {
      int retValue = cellManager.decryptInt(cellManager.encryptInt(value, BasicCellManager.BASIC_PROVIDER_VAL));
      assertEquals(value, retValue);
    }
  }

  @Test
  public void testBasicEncryptDecryptLong() {
    CellManager cellManager = new BasicCellManager();
    for (long value : new long[]{1, -1, Long.MAX_VALUE, Long.MIN_VALUE, 0}) {
      long retValue = cellManager.decryptLong(cellManager.encryptLong(value, BasicCellManager.BASIC_PROVIDER_VAL));
      assertEquals(value, retValue);
    }
  }

  @Test
  public void testEncryptDecryptBoolean() {
    CellManager cellManager = new BasicCellManager();
    assertEquals(
      true, cellManager.decryptBoolean(cellManager
        .encryptBoolean(true, BasicCellManager.BASIC_PROVIDER_VAL)));
    assertEquals(
      false, cellManager.decryptBoolean(cellManager
        .encryptBoolean(false, BasicCellManager.BASIC_PROVIDER_VAL)));
  }

  @Test
  public void testEncryptDecryptBinary() {
    CellManager cellManager = new BasicCellManager();

    // normal case
    byte[] bytes_1 =
      new byte[] {
        (byte) 0xe0,
        0x4f,
        (byte) 0xd0,
        (byte) 0x20,
        (byte) 0xea,
        (byte) 0x3a,
        (byte) 0x69,
        0x10,
        (byte) 0xa2,
        (byte) 0xd8,
        0x08,
        0x00,
        0x2b,
        0x30,
        0x30,
        (byte) 0x9d
      };
    Binary value_1 = Binary.fromConstantByteArray(bytes_1);
    Binary retValue_1 =
      cellManager.decryptBinary(cellManager.encryptBinary(value_1, BasicCellManager.BASIC_PROVIDER_VAL));
    assertEquals(value_1, retValue_1);

    // corner case
    byte[] bytes_2 = new byte[] {};
    Binary value_2 = Binary.fromConstantByteArray(bytes_2);
    Binary retValue_2 =
      cellManager.decryptBinary(cellManager.encryptBinary(value_2, BasicCellManager.BASIC_PROVIDER_VAL));
    assertEquals(value_2, retValue_2);
  }
}
