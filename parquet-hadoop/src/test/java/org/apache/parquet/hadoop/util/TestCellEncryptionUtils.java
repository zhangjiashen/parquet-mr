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
import org.apache.parquet.column.impl.BasicCellManager;
import org.apache.parquet.schema.ExtType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.parquet.crypto.CellEncryptionTest.addMappingToMockCellManager;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

public class TestCellEncryptionUtils {

  @Test
  public void testGetAllOriginalColumnDescriptors() {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, INT32, "trip_id"),
      new GroupType(REPEATED, "array",
        new GroupType(REPEATED, "list",
          new PrimitiveType(OPTIONAL, BINARY, "provider"),
          new GroupType(REPEATED, "points",
            new PrimitiveType(OPTIONAL, INT64, "lat"),
            new PrimitiveType(OPTIONAL, BINARY, "_lat_cell_encr"),
            new GroupType(REPEATED, "longlist",
              new PrimitiveType(OPTIONAL, INT64, "long"),
              new PrimitiveType(OPTIONAL, BINARY, "_long_cell_encr"))))));

    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"array", "list", "points", "lat"},
      new String[]{"array", "list", "points", "_lat_cell_encr"},
      new String[]{"array", "list", "provider"},
      new String[]{"array", "list", "points", "longlist", "long"},
      new String[]{"array", "list", "points", "longlist", "_long_cell_encr"},
      new String[]{"array", "list", "provider"});

    Set<ColumnDescriptor> origCols = CellEncryptionUtils.getAllOriginalColumnDescriptors(cellManager, schema);
    assertEquals(origCols.size(), 2);
    List<ColumnDescriptor> origColList = new ArrayList<>(origCols);
    assertArrayEquals(origColList.get(0).getPath(), new String[]{"array", "list", "points", "lat"});
    assertArrayEquals(origColList.get(1).getPath(), new String[]{"array", "list", "points", "longlist", "long"});
  }

  @Test(expected = IllegalStateException.class)
  public void testRepeatedProviderWrongSubtree() {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new GroupType(REPEATED, "list",
        new PrimitiveType(OPTIONAL, BINARY, "provider")),
      new GroupType(REPEATED, "points",
        new PrimitiveType(OPTIONAL, FLOAT, "lat"),
        new PrimitiveType(OPTIONAL, BINARY, "_lat_cell_encr")));
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"points", "lat"},
      new String[]{"points", "_lat_cell_encr"},
      new String[]{"list", "provider"});

    CellEncryptionUtils.validateCellManagerWithSchema(cellManager, schema);
  }

  @Test(expected = IllegalStateException.class)
  public void testOriginalSiblingOfProviderRepeatedAncestor() {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new GroupType(REPEATED, "locs",
        new PrimitiveType(OPTIONAL, INT64, "lat"),
        new PrimitiveType(OPTIONAL, BINARY, "_lat_cell_encr"),
        new GroupType(REPEATED, "places",
          new PrimitiveType(OPTIONAL, BINARY, "provider"))));
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"locs", "lat"},
      new String[]{"locs", "_lat_cell_encr"},
      new String[]{"locs", "places", "provider"});

    CellEncryptionUtils.validateCellManagerWithSchema(cellManager, schema);
  }

  @Test(expected = IllegalStateException.class)
  public void testProviderRepeatedLeaf() {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, INT64, "lat"),
      new PrimitiveType(REPEATED, BINARY, "provider"),
      new PrimitiveType(OPTIONAL, BINARY, "_lat_cell_encr"));
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"},
      new String[]{"_lat_cell_encr"},
      new String[]{"provider"});

    CellEncryptionUtils.validateCellManagerWithSchema(cellManager, schema);
  }

  @Test
  public void testNoRepetitionCase() {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, INT64, "lat"),
      new PrimitiveType(OPTIONAL, BINARY, "provider"),
      new PrimitiveType(OPTIONAL, BINARY, "_lat_cell_encr"));
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"},
      new String[]{"_lat_cell_encr"},
      new String[]{"provider"});

    CellEncryptionUtils.validateCellManagerWithSchema(cellManager, schema);
  }

  @Test
  public void testRepeatedProviderHappyCase() {
    MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, INT32, "trip_id"),
      new GroupType(REPEATED, "array",
        new GroupType(REPEATED, "list",
          new PrimitiveType(OPTIONAL, BINARY, "provider"),
          new GroupType(REPEATED, "points",
            new PrimitiveType(OPTIONAL, INT64, "lat"),
            new PrimitiveType(OPTIONAL, BINARY, "_lat_cell_encr"),
            new GroupType(REPEATED, "longlist",
              new PrimitiveType(OPTIONAL, INT64, "long"),
              new PrimitiveType(OPTIONAL, BINARY, "_long_cell_encr"))))));

    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"array", "list", "points", "lat"},
      new String[]{"array", "list", "points", "_lat_cell_encr"},
      new String[]{"array", "list", "provider"},
      new String[]{"array", "list", "points", "longlist", "long"},
      new String[]{"array", "list", "points", "longlist", "_long_cell_encr"},
      new String[]{"array", "list", "provider"});
    CellEncryptionUtils.validateCellManagerWithSchema(cellManager, schema);

    // Verify it is still happy case after converted to ExtType schema
    MessageType extTypeSchema = convertToExtType(schema);
    CellEncryptionUtils.validateCellManagerWithSchema(cellManager, extTypeSchema);
  }


  private static MessageType convertToExtType(MessageType messageType) {
    List<Type> newFields = new ArrayList<>();
    for (Type field : messageType.getFields()) {
      ExtType<Object> cryptoField = convertToExtType(field);
      newFields.add(cryptoField);
    }

    return new MessageType(messageType.getName(), newFields);
  }

  private static ExtType<Object> convertToExtType(Type field) {
    ExtType<Object> result = null;
    if (field.isPrimitive()) {
      result = new ExtType<>(field);
    } else {
      List<Type> newFields = new ArrayList<>();
      for (Type childField : field.asGroupType().getFields()) {
        ExtType<Object> newField = convertToExtType(childField);
        newFields.add(newField);
      }
      result = new ExtType<>(field.asGroupType().withNewFields(newFields));
    }

    return result;
  }

}
