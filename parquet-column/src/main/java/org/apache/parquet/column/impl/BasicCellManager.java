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
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * BasicCellManager provides a lightweight implementation of CellManager for test purposes
 * without needing to provide a plugin or external implementation.
 * This class "encrypts" and "decrypts" by converting values to byte arrays and back
 * without actually using any crypto.
 *
 *  DO NOT USE FOR PRODUCTION - BasicCellManager encryption is not secure is meant
 *  for example / testing purposes only.
 */
public class BasicCellManager implements CellManager {
  public static final String BASIC_ENCRYPT_PREFIX = "basic_encrypt";
  public static final String BASIC_PROVIDER_VAL = "skjv";
  private static final byte[] PREFIX_BYTES = BASIC_ENCRYPT_PREFIX.getBytes(StandardCharsets.UTF_8);

  public BasicCellManager() {
  }

  @Override
  public CellColumnType getColumnType(ColumnPath column) {
    return CellColumnType.NONE;
  }

  @Override
  public ColumnPath getOriginalColumn(ColumnPath columnPath) {
    return null;
  }

  @Override
  public ColumnPath getEncryptedColumn(ColumnPath columnPath) {
    return null;
  }

  @Override
  public ColumnPath getProviderColumn(ColumnPath columnPath) {
    return null;
  }

  @Override
  public Set<ColumnPath> getOriginalColumns() {
    return new HashSet<>();
  }

  @Override
  public boolean isPassThroughModeEnabled() {
    return false;
  }

  @Override
  public float decryptFloat(byte[] value) {
    int prefixEndIndex = PREFIX_BYTES.length;
    ByteBuffer bb = ByteBuffer.wrap(value, prefixEndIndex, Float.BYTES);
    return bb.getFloat();
  }

  @Override
  public byte[] encryptFloat(float value, String provider) {
    if (shouldIEncrypt(provider)) {
      int typeSize = Float.BYTES;
      return ByteBuffer.allocate(PREFIX_BYTES.length + typeSize)
        .put(PREFIX_BYTES).putFloat(value).array();
    }
    return null;
  }

  @Override
  public double decryptDouble(byte[] value) {
    int prefixEndIndex = PREFIX_BYTES.length;
    ByteBuffer bb = ByteBuffer.wrap(value, prefixEndIndex, Double.BYTES);
    return bb.getDouble();
  }

  @Override
  public byte[] encryptDouble(double value, String provider) {
    if (shouldIEncrypt(provider)) {
      int typeSize = Double.BYTES;
      return ByteBuffer.allocate(PREFIX_BYTES.length + typeSize)
        .put(PREFIX_BYTES).putDouble(value).array();
    }
    return null;
  }

  @Override
  public int decryptInt(byte[] value) {
    int prefixEndIndex = PREFIX_BYTES.length;
    ByteBuffer bb = ByteBuffer.wrap(value, prefixEndIndex, Integer.BYTES);
    return bb.getInt();
  }

  @Override
  public byte[] encryptInt(int value, String provider) {
    if (shouldIEncrypt(provider)) {
      int typeSize = Integer.BYTES;
      return ByteBuffer.allocate(PREFIX_BYTES.length + typeSize)
        .put(PREFIX_BYTES).putInt(value).array();
    }
    return null;
  }

  @Override
  public long decryptLong(byte[] value) {
    int prefixEndIndex = PREFIX_BYTES.length;
    ByteBuffer bb = ByteBuffer.wrap(value, prefixEndIndex, Long.BYTES);
    return bb.getLong();
  }

  @Override
  public byte[] encryptLong(long value, String provider) {
    if (shouldIEncrypt(provider)) {
      int typeSize = Long.BYTES;
      return ByteBuffer.allocate(PREFIX_BYTES.length + typeSize)
        .put(PREFIX_BYTES).putLong(value).array();
    }
    return null;
  }

  @Override
  public Binary decryptBinary(byte[] value) {
    int prefixEndIndex = PREFIX_BYTES.length;
    byte[] binArr = Arrays.copyOfRange(value, prefixEndIndex, value.length);
    return Binary.fromConstantByteArray(binArr);
  }

  @Override
  public byte[] encryptBinary(Binary value, String provider) {
    if (shouldIEncrypt(provider)) {
      int typeSize = value.getBytes().length;
      return ByteBuffer.allocate(PREFIX_BYTES.length + typeSize)
        .put(PREFIX_BYTES).put(value.getBytes()).array();
    }
    return null;
  }

  @Override
  public boolean decryptBoolean(byte[] value) {
    int prefixEndIndex = PREFIX_BYTES.length;
    ByteBuffer bb = ByteBuffer.wrap(value, prefixEndIndex, Character.BYTES);
    return (bb.getChar() == 'T');
  }

  @Override
  public byte[] encryptBoolean(boolean value, String provider) {
    if (shouldIEncrypt(provider)) {
      int typeSize = Character.BYTES;
      return ByteBuffer.allocate(PREFIX_BYTES.length + typeSize)
        .put(PREFIX_BYTES).putChar(value ? 'T' : 'F').array();
    }
    return null;
  }

  private boolean shouldIEncrypt(String provider) {
    return provider != null && provider.toLowerCase().equals(BASIC_PROVIDER_VAL);
  }
}
