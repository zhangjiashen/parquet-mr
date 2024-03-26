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
package org.apache.parquet.hadoop.util;

import org.apache.parquet.column.CellManager;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;


public class CellEncryptionUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CellEncryptionUtils.class);
  private static final boolean DEBUG = LOG.isDebugEnabled();
  private static final String errorMessageNotSiblings = "Configured ORIGINAL column = [ %s ] " +
    "and ENCRYPTED column = [ %s ] for cell encryption must be sibling columns in the schema";
  private static final String errorMessageEncrProvNotExists = "Configured ENCRYPTED = [ %s ] or" +
    " PROVIDER = [ %s ] columns do not exist in the schema";
  private static final String errorMessageOpt = "One or more configured cell encryption column(s) = [ %s, %s ]" +
    " are REQUIRED fields. Fields must be OPTIONAL/REPEATED";
  private static final String errorMessageTwiceSpecified = "Same cell ENCRYPTED column = [ %s ] specified for more than" +
    " one ORIGINAL columns";
  private static final String errorMessageInvalidRepeatedProvider = "Invalid REPEATED provider + original column" +
    " layout. Original column must be in the subtree of the provider column's nearest REPEATED ancestor." +
    " Provider col = [ %s ] and Original col = [ %s ] and REPEATED ancestor = [ %s ]";

  private static void validateOriginalEncryptedSiblings(String[] origPath, String[] encrPath) {
    if (origPath.length != encrPath.length) {
      throw new IllegalStateException(String.format(errorMessageNotSiblings,
        Arrays.toString(origPath), Arrays.toString(encrPath)));
    }
    int parentLength = origPath.length - 1;
    for(int i = 0; i < parentLength; i++) {
      if (!origPath[i].equals(encrPath[i])) {
        throw new IllegalStateException(String.format(errorMessageNotSiblings,
          Arrays.toString(origPath), Arrays.toString(encrPath)));
      }
    }
  }

  private static void validateEncrProvColumnsExists(MessageType schema, String[] encrColPath, String[] provColPath) {
    if (!schema.containsPath(encrColPath) || !schema.containsPath(provColPath)) {
      throw new IllegalStateException(String.format(errorMessageEncrProvNotExists,
        Arrays.toString(encrColPath), Arrays.toString(provColPath)));
    }
  }

  private static boolean checkFirstNArrayEquals(String[] arr1, String[] arr2, int n) {
    if (n > arr1.length || n > arr2.length) {
      return false;
    }
    for (int i = 0; i < n; i++) {
      if (!arr1[i].equals(arr2[i])) {
        return false;
      }
    }
    return true;
  }

  // Throw an error when original columns are not in the subtree of the provider's
  // nearest REPEATED ancestor
  private static void validateRepeatedProviderSchema(MessageType schema,
                                                     String[] origPath, String[] provPath) {
    if (DEBUG) LOG.debug("validateRepeatedProviderSchema: for schema = {}, originalColPath = {}" +
      " and providerColPath = {}", schema.toString(), Arrays.toString(origPath), Arrays.toString(provPath));
    int nearestRepeatedAncestorIndex = -1;
    Type ancestor = schema;
    for (int i = 0; i < provPath.length; i++) {
      if (ancestor.isPrimitive()) {
        throw new IllegalStateException(String.format("ancestor node = [ %s ]" +
          " is expected to be groupType", ancestor.toString()));
      }
      ancestor = ancestor.asGroupType().getType(provPath[i]);
      if (ancestor.getRepetition() == Type.Repetition.REPEATED) {
        if (DEBUG) LOG.debug("REPEATED ancestor found - ancestor = {}", ancestor);
        nearestRepeatedAncestorIndex = i;
      }
    }

    if (!checkFirstNArrayEquals(origPath, provPath, nearestRepeatedAncestorIndex+1)) {
      if (nearestRepeatedAncestorIndex == -1) {
        throw new IllegalStateException("Invalid state: provider and original column path not matching" +
          " for nearestRepeatedAncestor = -1. This state is not possible without code defect.");
      }
      throw new IllegalStateException(String.format(errorMessageInvalidRepeatedProvider,
        Arrays.toString(provPath), Arrays.toString(origPath), provPath[nearestRepeatedAncestorIndex]));
    }
  }

  /**
   * Return all columns configured for cell encryption in the CellManager for a given schema.
   * @return Set of ColumnDescriptors configured for cell encryption.
   */
  public static Set<ColumnDescriptor> getAllOriginalColumnDescriptors(CellManager cellManager, MessageType schema) {
    return cellManager.getOriginalColumns().stream()
      .map(path -> schema.getColumnDescription(path.toArray()))
      .collect(Collectors.toSet());
  }

  /**
   * Validates RootComply mappings against the schema and throws error when invalid state is detected
   * Possible invalid states:
   * 1. Configured ORIGINAL or ENCRYPTED columns are REQUIRED
   * 2. Two ORIGINAL columns are writing data to the same ENCRYPTED column (data loss possible).
   * 3. Configured ENCRYPTED / PROVIDER columns do not exist in schema
   * 4. ORIGINAL and ENCRYPTED columns are not siblings in the schema
   * 5. Original column not in the subtree of the provider column's nearest REPEATED ancestor.
   */
  public static void validateCellManagerWithSchema(CellManager cellManager, MessageType schema) {
    if (schema == null) {
      throw new IllegalStateException("MessageType schema is null - cannot validate for CellEncryption");
    }
    if (cellManager != null) {
      Set<ColumnDescriptor> originalColumns = getAllOriginalColumnDescriptors(cellManager, schema);

      Set<String> encrColDotPaths = new HashSet<>();
      for (ColumnDescriptor originalCol : originalColumns) {
        String[] originalPath = originalCol.getPath();
        ColumnDescriptor origColDesc = schema.getColumnDescription(originalPath);
        String[] encrColPath = cellManager.getEncryptedColumn(originalPath);
        // Skip provider column validation if pass through mode is enabled.
        // Pass through writes are already encrypted so provider column is not needed.
        if (!cellManager.isPassThroughModeEnabled()) {
          String[] providerColPath = cellManager.getProviderColumn(originalPath);
          validateEncrProvColumnsExists(schema, encrColPath, providerColPath);
          validateRepeatedProviderSchema(schema, originalPath, providerColPath);
        }

        Type origColType = origColDesc.getPrimitiveType();
        Type encrColType = schema.getType(encrColPath);
        if (origColType.getRepetition() == Type.Repetition.REQUIRED
          || encrColType.getRepetition() == Type.Repetition.REQUIRED) {
          throw new IllegalStateException(String.format(errorMessageOpt, origColType, encrColType));
        }

        validateOriginalEncryptedSiblings(originalPath, encrColPath);

        String encrDotString = ColumnPath.get(encrColPath).toDotString();
        if (encrColDotPaths.contains(encrDotString)) {
          throw new IllegalStateException(String.format(errorMessageTwiceSpecified, encrDotString));
        }
        encrColDotPaths.add(encrDotString);
      }
    }
  }

}
