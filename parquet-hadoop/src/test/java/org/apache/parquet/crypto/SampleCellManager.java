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
package org.apache.parquet.crypto;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.impl.BasicCellManager;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;

public class SampleCellManager extends BasicCellManager {
  public static final String ENCRYPTED_COLUMN_SUFFIX = "_encr"; // test column name convention

  private final Map<ColumnPath, ColumnPath> originalToEncryptedColumn = new HashMap<>();
  private final Map<ColumnPath, ColumnPath> encryptedToOriginalColumn = new HashMap<>();

  public SampleCellManager(Configuration conf, MessageType fileSchema) {
    Set<ColumnPath> allPaths = fileSchema.getPaths().stream().map(ColumnPath::get).collect(Collectors.toSet());

    for (ColumnPath path: allPaths) {
      String[] arr = path.toArray();
      String last = arr[arr.length - 1];
      if (last.endsWith(ENCRYPTED_COLUMN_SUFFIX)) {
        String[] copy = Arrays.copyOf(arr, arr.length);
        copy[copy.length - 1] = last.substring(0, last.length() - ENCRYPTED_COLUMN_SUFFIX.length());
        ColumnPath original = ColumnPath.get(copy);
        if (allPaths.contains(original)) {
          originalToEncryptedColumn.put(original, path);
          encryptedToOriginalColumn.put(path, original);
        }
      }
    }
  }

  @Override
  public ColumnPath getOriginalColumn(ColumnPath columnPath) {
    return encryptedToOriginalColumn.get(columnPath);
  }

  @Override
  public ColumnPath getEncryptedColumn(ColumnPath columnPath) {
    return originalToEncryptedColumn.get(columnPath);
  }

  @Override
  public Set<ColumnPath> getOriginalColumns() {
    return originalToEncryptedColumn.keySet();
  }
}
