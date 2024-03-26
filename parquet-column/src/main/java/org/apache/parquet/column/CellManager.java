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

import java.util.Set;

import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

public interface CellManager {

  /**
   * Given ColumnPath, this method returns CellColumnType defined below.
   * This helps check the encryption status of a column.
   * @param columnPath column path
   * @return CellColumnType
   */
  CellColumnType getColumnType(ColumnPath columnPath);

  /**
   * Get the corresponding original column path if the given column path is a
   * cell encrypted column.
   *
   * @param columnPath A column path.
   * @return The corresponding original column path if given column is a cell
   * encrypted column, or null otherwise.
   */
  ColumnPath getOriginalColumn(ColumnPath columnPath);

  /**
   * Get the corresponding original column path if the given column path is a
   * cell encrypted column.
   */
  default String[] getOriginalColumn(String[] columnPath) {
    ColumnPath path = getOriginalColumn(ColumnPath.get(columnPath));
    return path != null ? path.toArray() : null;
  }

  /**
   * Get the corresponding cell encryption encrypted column path if the given
   * column path is an original column. The encrypted column is expected to be
   * of string type and the same nested level and repetition as original column.
   *
   * @param columnPath A column path.
   * @return The corresponding cell encrypted column path for the given original
   * column, or null if not exist.
   */
  ColumnPath getEncryptedColumn(ColumnPath columnPath);

  /**
   * Get the corresponding cell encryption encrypted column path if the given
   * column path is an original column.
   */
  default String[] getEncryptedColumn(String[] columnPath) {
    ColumnPath path = getEncryptedColumn(ColumnPath.get(columnPath));
    return path != null ? path.toArray() : null;
  }

  /**
   * Get the corresponding cell encryption provider column path if the given
   * column path is an original column. The provider column is expected to be of
   * string type.
   *
   * @param columnPath A column path.
   * @return The corresponding provider column path for the given original
   * column, or null if not exist.
   */
  ColumnPath getProviderColumn(ColumnPath columnPath);

  /**
   * Get the corresponding cell encryption provider column path if the given
   * column path is an original column.
   */
  default String[] getProviderColumn(String[] columnPath) {
    ColumnPath path = getProviderColumn(ColumnPath.get(columnPath));
    return path != null ? path.toArray() : null;
  }

  /**
   * Get all original columns paths.
   * @return A set of all original columns paths. Empty set if none.
   */
  Set<ColumnPath> getOriginalColumns();

  /**
   * Convenience helper same as CellManagerUtils.addMissingRequestedColumns.
   */
  default MessageType addMissingRequestedColumns(
    MessageType requestedSchema, MessageType fileSchema) {
    return CellManagerUtils.addMissingRequestedColumns(this, requestedSchema, fileSchema);
  }

  /**
   * Pass through mode ("ingestion mode") indicates Parquet should write cell encryption
   * columns without doing any encryption. This is useful for ingesting pre cell encrypted data
   * from another data source. When mode is enabled, data will be written directly
   * but statistics/dictionary encoding will be handled correctly by parquet.
   * @return true if passThrough mode is enabled, false otherwise.
   */
  boolean isPassThroughModeEnabled();

  /**
   * Method to decrypt a float value.
   * @param value encrypted value
   * @return float value
   */
  float decryptFloat(byte[] value);

  /**
   * Method to encrypt a float value.
   * @param value float value
   * @param provider provider (nullable)
   * @return encrypted value, or null
   * if encryption not required for this provider/context.
   */
  byte[] encryptFloat(float value, String provider);

  /**
   * Method to decrypt a double value.
   * @param value encrypted value
   * @return double value
   */
  double decryptDouble(byte[] value);

  /**
   * Method to encrypt a double value.
   * @param value double value
   * @param provider provider (nullable)
   * @return encrypted value, or null
   * if encryption not required for this provider/context.
   */
  byte[] encryptDouble(double value, String provider);

  /**
   * Method to decrypt an int value.
   * @param value encrypted value
   * @return int value
   */
  int decryptInt(byte[] value);

  /**
   * Method to encrypt an int value.
   * @param value int value
   * @param provider provider (nullable)
   * @return encrypted value, or null
   * if encryption not required for this provider/context.
   */
  byte[] encryptInt(int value, String provider);

  /**
   * Method to decrypt a long value.
   * @param value encrypted value
   * @return long value
   */
  long decryptLong(byte[] value);

  /**
   * Method to encrypt a long value.
   * @param value long value
   * @param provider provider (nullable)
   * @return encrypted value, or null
   * if encryption not required for this provider/context.
   */
  byte[] encryptLong(long value, String provider);

  /**
   * Method to decrypt a Binary.
   * @param value encrypted value
   * @return Binary value
   */
  Binary decryptBinary(byte[] value);

  /**
   * Method to encrypt a Binary.
   * @param value Binary value
   * @param provider provider (nullable)
   * @return encrypted value, or null
   * if encryption not required for this provider/context.
   */
  byte[] encryptBinary(Binary value, String provider);

  /**
   * Method to decrypt a Boolean.
   * @param value encrypted value
   * @return boolean value
   */
  boolean decryptBoolean(byte[] value);

  /**
   * Method to encrypt a boolean value.
   * @param value boolean value
   * @param provider provider (nullable)
   * @return encrypted value, or null
   * if encryption not required for this provider/context.
   */
  byte[] encryptBoolean(boolean value, String provider);

  /**
   * This defines encryption status of a column.
   */
  public enum CellColumnType {
    NONE,
    ORIGINAL,
    ENCRYPTED,
    PROVIDER,
  }

  /**
   * Encryption key is not found. Could be that the key has never been created,
   * or has been "soft deleted" (key expired) or physically deleted.
   *
   * For reader, this means data encrypted with this key is no longer available
   * so reader will just return nulls.
   */
  public class KeyNotFoundException extends ParquetRuntimeException {
    public KeyNotFoundException() {
    }

    public KeyNotFoundException(String message) {
      super(message);
    }

    public KeyNotFoundException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Data encryption / decryption crypto error. This is typically caused by data
   * corruption.
   */
  public class CryptoException extends ParquetRuntimeException {
    public CryptoException() {
    }

    public CryptoException(String message) {
      super(message);
    }

    public CryptoException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Access to given cell data is denied. This could be user does not have
   * access to the encryption key.
   */
  public class AccessDeniedException extends ParquetRuntimeException {
    public AccessDeniedException() {
    }

    public AccessDeniedException(String message) {
      super(message);
    }

    public AccessDeniedException(String message, Throwable cause) {
      super(message, cause);
    }
  }

}
