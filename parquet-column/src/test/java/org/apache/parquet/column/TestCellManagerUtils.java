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
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCellManagerUtils {

  @Test
  public void testAddMissingRequestedColumns() {
    MessageType fileSchema = parseMessageType("message file {"
      + "  optional double lng;"
      + "  optional binary _lng_cell_encr;"
      + "  optional double lat;"
      + "  optional binary _lat_cell_encr;"
      + "  repeated group foo {"
      + "    optional double lng;"
      + "    optional binary _lng_cell_encr;"
      + "    optional double lat;"
      + "    optional binary _lat_cell_encr;"
      + "  }"
      + "  optional binary other;"
      + "}");
    MessageType requestedSchema = parseMessageType("message request {"
      + "  optional double lng;"
      + "  optional double lat;"
      + "  optional binary _lat_cell_encr;"
      + "  repeated group foo {"
      + "    optional double lng;"
      + "    optional double lat;"
      + "    optional binary _lat_cell_encr;"
      + "  }"
      + "  required binary bar;"
      + "}");

    // null CellManager, no change
    Assert.assertSame(requestedSchema,
      CellManagerUtils.addMissingRequestedColumns(null, requestedSchema, fileSchema));

    // CellManager reports no columns, no change
    CellManager cellManager = mock(CellManager.class);
    Assert.assertSame(requestedSchema,
      CellManagerUtils.addMissingRequestedColumns(cellManager, requestedSchema, fileSchema));

    when(cellManager.getEncryptedColumn(ColumnPath.get("lng")))
      .thenReturn(ColumnPath.get("_lng_cell_encr"));
    when(cellManager.getEncryptedColumn(ColumnPath.get("lat")))
      .thenReturn(ColumnPath.get("_lat_cell_encr"));
    when(cellManager.getEncryptedColumn(ColumnPath.get("foo", "lng")))
      .thenReturn(ColumnPath.get("foo", "_lng_cell_encr"));
    when(cellManager.getEncryptedColumn(ColumnPath.get("foo", "lat")))
      .thenReturn(ColumnPath.get("foo", "_lat_cell_encr"));

    // 2 missing encrypted columns should be appended to the end
    MessageType expected = parseMessageType("message request {"
      + "  optional double lng;"
      + "  optional double lat;"
      + "  optional binary _lat_cell_encr;"
      + "  repeated group foo {"
      + "    optional double lng;"
      + "    optional double lat;"
      + "    optional binary _lat_cell_encr;"
      + "    optional binary _lng_cell_encr;"
      + "  }"
      + "  required binary bar;"
      + "  optional binary _lng_cell_encr;"
      + "}");
    assertEquals(expected,
      CellManagerUtils.addMissingRequestedColumns(cellManager, requestedSchema, fileSchema));

    // no missing, no change
    Assert.assertSame(fileSchema,
      CellManagerUtils.addMissingRequestedColumns(cellManager, fileSchema, fileSchema));
    Assert.assertSame(expected,
      CellManagerUtils.addMissingRequestedColumns(cellManager, expected, fileSchema));
  }

  @Test
  public void testNestedLeafEncryptedOnlyRequestSchema() {
    MessageType fileSchema = parseMessageType("message spark_schema {" +
      "  optional group msg {" +
      "    optional group waypoints (LIST) {" +
      "      repeated group array {" +
      "        optional group location {" +
      "          optional double latitude;" +
      "          optional binary latitude_cell_encr;" +
      "        }" +
      "      }" +
      "    }" +
      "  }" +
      "}");

    MessageType requestSchema = parseMessageType("message spark_schema {" +
      "  optional group msg {" +
      "    optional group waypoints (LIST) {" +
      "      repeated group array {" +
      "        optional group location {" +
      "          optional double latitude;" +
      "        }" +
      "      }" +
      "    }" +
      "  }" +
      "}");
    CellManager cellManager = mock(CellManager.class);
    when(cellManager.getEncryptedColumn(ColumnPath.get("msg", "waypoints", "array", "location", "latitude")))
      .thenReturn(ColumnPath.get("msg", "waypoints", "array", "location", "latitude_cell_encr"));

    assertEquals(fileSchema, CellManagerUtils.addMissingRequestedColumns(cellManager, requestSchema, fileSchema));
  }
}
