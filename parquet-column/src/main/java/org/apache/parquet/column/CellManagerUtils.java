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

import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CellManagerUtils {

  /**
   * Add missing encrypted columns into request schema required by cell
   * encryption enabled parquet reader.
   *
   * Cell encryption enabled parquet reader needs to read encrypted columns in
   * order to materialize original column values correctly. This requires
   * request schema to include encrypted column for any requested original
   * column.
   *
   * This function examines request schema, finds any missing encrpted columns
   * and copy them from the file schema, appends to the end of corresponding
   * parent group fields.
   *
   * WARNING: The result full request schema can be different from client's
   * actual request schema. The result full request schema needs to be used to
   * read parquet files, while client's original request schema is used by
   * client for record materializer. This works based on 2 designs:
   *
   * (1) The result full schema is "compatible" with client's request schema in
   * that all missing columns are only appended to the end of parent group. All
   * existing fields' positions are unchanged and match client's record
   * materializer.
   *
   * (2) Cell encryption enabled parquet reader redirects encrypted column reads
   * to original column record materializer. The added missing column read
   * result will never go to non-exist record materializer.
   *
   * @param cellManager     optional CellManager providing original/encrypted
   *                        column mappings.
   * @param requestedSchema client's actual request schema
   * @param fileSchema      parquet file schema
   * @return a full request schema by copying missing encrypted columns from
   *         file schema to client's request schema. Or client's request schema
   *         if no change needed.
   */
  public static MessageType addMissingRequestedColumns(CellManager cellManager,
                                                       MessageType requestedSchema, MessageType fileSchema) {
    if (cellManager == null) {
      return requestedSchema;
    }

    Set<ColumnPath> requested = requestedSchema.getPaths().stream()
      .map(ColumnPath::get).collect(Collectors.toSet());
    Set<ColumnPath> missing = requested.stream().map(path -> {
      ColumnPath encrypted = cellManager.getEncryptedColumn(path);
      return encrypted != null && !requested.contains(encrypted) ? encrypted : null;
    }).filter(p -> p != null).collect(Collectors.toSet());
    if (missing.isEmpty()) {
      return requestedSchema;
    }

    Set<String> prefixes = new HashSet<>();
    Map<String, List<String>> leaves = new HashMap<>();
    for (ColumnPath path : missing) {
      String[] arr = path.toArray();
      String finalPrefix = Arrays.stream(arr).limit(arr.length - 1).reduce("", (prefix, cur) -> {
        prefix = prefix + "." + cur;
        prefixes.add(prefix);
        return prefix;
      });
      String leaf = arr[arr.length - 1];
      leaves.computeIfAbsent(finalPrefix, k -> new ArrayList<String>()).add(leaf);
    }
    return new MessageType(requestedSchema.getName(),
      addMissingFields(requestedSchema, fileSchema, "", prefixes, leaves));
  }

  private static List<Type> addMissingFields(
    GroupType requested, GroupType file, String currentPath,
    Set<String> prefixes, Map<String, List<String>> leaves) {
    List<Type> newFields = new ArrayList<Type>();
    for (Type type : requested.getFields()) {
      Type newType = type;
      if (!type.isPrimitive()) {
        GroupType group = type.asGroupType();
        String groupPath = currentPath + "." + group.getName();
        if (prefixes.contains(groupPath)) {
          newType = group.withNewFields(
            addMissingFields(
              group, file.getType(group.getName()).asGroupType(), groupPath, prefixes, leaves));
        }
      }
      newFields.add(newType);
    }

    // IMPORTANT: only append the missing leaves at the end of parent fields, so
    // that result is "compatible" with client's request schema
    if (leaves.containsKey(currentPath)) {
      // Skip adding to newFields if current path does not
      // have an encrypted col leaf.
      for (String name : leaves.get(currentPath)) {
        newFields.add(file.getType(name));
      }
    }

    return newFields;
  }

}
