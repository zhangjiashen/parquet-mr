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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.CellManager;
import org.apache.parquet.column.impl.BasicCellManager;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;

import static org.apache.parquet.Preconditions.checkArgument;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class CellEncryptionTest {
  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();
  private final static Logger LOG = LoggerFactory.getLogger(CellEncryptionTest.class);
  private final static String providerEncryptFlag = "skjv";
  private final static int numRecord = 500;
  private final static int arrayElements = 5;

  private Map<String, Object[]> skjvTestData = new HashMap<>();
  private Random rnd = new Random(5);

  @Before
  public void generateSkjvTestData() {
    String[] names = new String[numRecord];
    String[] providers = new String[numRecord];
    Long[] lats = new Long[numRecord];
    Double[] doubles = new Double[numRecord];
    Integer[] integers = new Integer[numRecord];
    Float[] floats = new Float[numRecord];
    String[] lats_encrypted = new String[numRecord];
    String[] linkedInWebs = new String[numRecord];
    String[] twitterWebs = new String[numRecord];
    Boolean[] optionalFields = new Boolean[numRecord];
    Binary[] binaries = new Binary[numRecord];
    for (int i = 0; i < numRecord; i++) {
      names[i] = getString();
      providers[i] = getProviders();
      lats[i] = getLong();
      doubles[i] = getDouble();
      integers[i] = getInteger();
      floats[i] = getFloat();
      lats_encrypted[i] = "garbage_value";
      linkedInWebs[i] = getString();
      twitterWebs[i] = getString();
      optionalFields[i] = rnd.nextBoolean();
      binaries[i] = Binary.fromString(getString(12)); // INT96 len
    }

    skjvTestData.put("name", names);
    skjvTestData.put("nickname", names);
    skjvTestData.put("lat_provider", providers);
    skjvTestData.put("lat", lats);
    skjvTestData.put("lat_encrypted", lats_encrypted);
    skjvTestData.put("long", lats);
    skjvTestData.put("long_provider", providers);
    skjvTestData.put("long_encrypted", lats_encrypted);
    skjvTestData.put("LinkedIn", linkedInWebs);
    skjvTestData.put("Twitter", twitterWebs);
    skjvTestData.put("optional_fields", optionalFields);
    skjvTestData.put("provider", providers);
    skjvTestData.put("double", doubles);
    skjvTestData.put("float", floats);
    skjvTestData.put("integer", integers);
    skjvTestData.put("binaries", binaries);
  }

  private String getProviders() {
    List<String> providers = Arrays.asList("uber", "skjv", "mcdonalds");
    return providers.get(rnd.nextInt(providers.size()));
  }

  private static long getLong() {
    return ThreadLocalRandom.current().nextLong(128);
  }

  private static double getDouble() {
    return ThreadLocalRandom.current().nextDouble();
  }

  private static float getFloat() {
    return ThreadLocalRandom.current().nextFloat();
  }

  private static int getInteger() {
    return ThreadLocalRandom.current().nextInt();
  }

  private String getString() {
    return getString(10);
  }
  private String getString(int len) {
    char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'x', 'z', 'y'};
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; i++) {
      sb.append(chars[rnd.nextInt(10)]);
    }
    return sb.toString();
  }

  private String writeCellEncryptedRequiredSubGroup(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, BINARY, "lat_encrypted"),
      new PrimitiveType(OPTIONAL, INT64, "lat"),
      new PrimitiveType(REQUIRED, BINARY, "lat_provider"),
      new GroupType(REQUIRED, "Locations",
        new PrimitiveType(REQUIRED, BINARY, "long_provider"),
        new PrimitiveType(OPTIONAL, INT64, "long"),
        new PrimitiveType(OPTIONAL, BINARY, "long_encrypted")));
    Builder builder = createBuilder(file, schema, cellManager, conf);

    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("lat_encrypted", (String)skjvTestData.get("lat_encrypted")[i]);
        g.add("lat", (Long)skjvTestData.get("lat")[i]);
        g.add("lat_provider", (String)skjvTestData.get("lat_provider")[i]);
        Group links = g.addGroup("Locations");
        links.add(0, (String)skjvTestData.get("long_provider")[i]);
        links.add(1, (Long)skjvTestData.get("long")[i]);
        links.add(2, (String)skjvTestData.get("long_encrypted")[i]);
        writer.write(g);
      }
    }
    return file;
  }

  private String writeCellEncryptedOptionalSubGroup(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(REQUIRED, BINARY, "name"),
      new PrimitiveType(REQUIRED, INT64, "lat"),
      new PrimitiveType(REQUIRED, BINARY, "nickname"),
      new GroupType(OPTIONAL, "Locations",
        new PrimitiveType(OPTIONAL, BINARY, "long_encrypted"),
        new PrimitiveType(REQUIRED, BINARY, "long_provider"),
        new PrimitiveType(OPTIONAL, INT64, "long")));
    Builder builder = createBuilder(file, schema, cellManager, conf);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("name", (String)skjvTestData.get("name")[i]);
        g.add("lat", (Long)skjvTestData.get("lat")[i]);
        g.add("nickname", (String)skjvTestData.get("nickname")[i]);
        if((Boolean)skjvTestData.get("optional_fields")[i]) {
          Group links = g.addGroup("Locations");
          links.add(0, (String)skjvTestData.get("long_encrypted")[i]);
          links.add(1, (String)skjvTestData.get("long_provider")[i]);
          links.add(2, (Long)skjvTestData.get("long")[i]);
        }
        writer.write(g);
      }
    }
    return file;
  }

  private String writeCellEncryptedDiffProviderLevel(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, BINARY, "name"),
      new PrimitiveType(OPTIONAL, INT64, "lat"),
      new PrimitiveType(OPTIONAL, BINARY, "lat_encrypted"),
      new GroupType(OPTIONAL, "Locations",
        new PrimitiveType(OPTIONAL, INT64, "long"),
        new PrimitiveType(OPTIONAL, BINARY, "long_encrypted"),
        new PrimitiveType(REQUIRED, BINARY, "lat_provider")),
      new PrimitiveType(REQUIRED, BINARY, "long_provider"));
    Builder builder = createBuilder(file, schema, cellManager, conf);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("name", (String)skjvTestData.get("name")[i]);
        g.add("lat", (Long)skjvTestData.get("lat")[i]);
        g.add("lat_encrypted", (String)skjvTestData.get("lat_encrypted")[i]);
        if((Boolean)skjvTestData.get("optional_fields")[i]) {
          // Randomly write / withhold optional field.
          Group links = g.addGroup("Locations");
          links.add(0, (Long)skjvTestData.get("long")[i]);
          links.add(1, (String)skjvTestData.get("long_encrypted")[i]);
          links.add(2, (String)skjvTestData.get("lat_provider")[i]);
        }
        g.add("long_provider", (String)skjvTestData.get("long_provider")[i]);
        writer.write(g);
      }
    }
    return file;
  }

  private MessageType getManySubGroupSchema() {
    return new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, BINARY, "name"),
      new GroupType(OPTIONAL, "Locations",
        new PrimitiveType(OPTIONAL, INT64, "long"),
        new PrimitiveType(OPTIONAL, BINARY, "_long_cell_encr"),
        new GroupType(OPTIONAL, "waypoints",
          new PrimitiveType(REPEATED, BINARY, "visitors"),
          new PrimitiveType(OPTIONAL, INT64, "lat"),
          new PrimitiveType(OPTIONAL, BINARY, "_lat_cell_encr"),
          new GroupType(OPTIONAL, "pois",
            new PrimitiveType(REPEATED, BINARY, "visitors"),
            new PrimitiveType(OPTIONAL, INT64, "placeid"),
            new PrimitiveType(OPTIONAL, BINARY, "_placeid_cell_encr")))),
      new PrimitiveType(REQUIRED, BINARY, "provider"));
  }

  private String writeManyNonRepeatedSubgroups(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = getManySubGroupSchema();
    Builder builder = createBuilder(file, schema, cellManager, conf);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("name", (String)skjvTestData.get("name")[i]);
        Group locations = g.addGroup("Locations");
        locations.add(0, (Long)skjvTestData.get("long")[i]);
        locations.add(1, (String)skjvTestData.get("long_encrypted")[i]);
        Group waypoints = locations.addGroup("waypoints");
        waypoints.add(0, (String)skjvTestData.get("name")[i]);
        waypoints.add(0, (String)skjvTestData.get("name")[i]);
        waypoints.add(1, (Long)skjvTestData.get("long")[i]);
        Group pois = waypoints.addGroup("pois");
        pois.add(0, (String)skjvTestData.get("name")[i]);
        pois.add(0, (String)skjvTestData.get("name")[i]);
        pois.add(1, (Long)skjvTestData.get("long")[i]);
        g.add("provider", providerEncryptFlag);
        writer.write(g);
      }
    }
    return file;
  }

  private String writeEncryptionManySubgroups(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, BINARY, "name"),
      new GroupType(REPEATED, "Locations",
        new PrimitiveType(OPTIONAL, INT64, "long"),
        new PrimitiveType(OPTIONAL, BINARY, "_long_cell_encr"),
        new GroupType(OPTIONAL, "waypoints",
          new PrimitiveType(REPEATED, BINARY, "visitors"),
          new PrimitiveType(OPTIONAL, INT64, "lat"),
          new PrimitiveType(OPTIONAL, BINARY, "_lat_cell_encr"),
          new GroupType(OPTIONAL, "pois",
            new PrimitiveType(REPEATED, BINARY, "visitors"),
            new PrimitiveType(OPTIONAL, INT64, "placeid"),
            new PrimitiveType(OPTIONAL, BINARY, "_placeid_cell_encr")))),
      new PrimitiveType(REQUIRED, BINARY, "provider"));
    Builder builder = createBuilder(file, schema, cellManager, conf);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("name", (String)skjvTestData.get("name")[i]);
        Group locations = g.addGroup("Locations");
        locations.add(0, (Long)skjvTestData.get("long")[i]);
        locations.add(1, (String)skjvTestData.get("long_encrypted")[i]);
        Group waypoints = locations.addGroup("waypoints");
        waypoints.add(0, (String)skjvTestData.get("name")[i]);
        waypoints.add(0, (String)skjvTestData.get("name")[i]);
        waypoints.add(1, (Long)skjvTestData.get("long")[i]);
        Group pois = waypoints.addGroup("pois");
        pois.add(0, (String)skjvTestData.get("name")[i]);
        pois.add(0, (String)skjvTestData.get("name")[i]);
        pois.add(1, (Long)skjvTestData.get("long")[i]);
        g.add("provider", providerEncryptFlag);
        Group location2 = g.addGroup("Locations");
        location2.add(0, (Long)skjvTestData.get("long")[i]);
        writer.write(g);
      }
    }
    return file;
  }

  private String writeSameProviderTwoColumns(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(REQUIRED, BINARY, "name"),
      new PrimitiveType(OPTIONAL, INT64, "lat"),
      new PrimitiveType(OPTIONAL, BINARY, "lat_encrypted"),
      new GroupType(OPTIONAL, "Locations",
        new PrimitiveType(OPTIONAL, INT64, "long"),
        new PrimitiveType(OPTIONAL, BINARY, "long_encrypted"),
        new PrimitiveType(REQUIRED, BINARY, "provider")));
    Builder builder = createBuilder(file, schema, cellManager, conf);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("name", (String)skjvTestData.get("name")[i]);
        g.add("lat", (Long)skjvTestData.get("lat")[i]);
        g.add("lat_encrypted", (String)skjvTestData.get("lat_encrypted")[i]);
        if((Boolean)skjvTestData.get("optional_fields")[i]) {
          // Randomly write / withhold optional field.
          Group links = g.addGroup("Locations");
          links.add(0, (Long)skjvTestData.get("long")[i]);
          links.add(1, (String)skjvTestData.get("long_encrypted")[i]);
          links.add(2, (String)skjvTestData.get("provider")[i]);
        }
        writer.write(g);
      }
    }
    return file;
  }

  private String writeArrayEncrypted(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(REQUIRED, BINARY, "name"),
      new PrimitiveType(REQUIRED, BINARY, "provider"),
      new GroupType(OPTIONAL, "Waypoints",
        new GroupType(REPEATED, "list",
          new GroupType(OPTIONAL, "element",
            new PrimitiveType(OPTIONAL, INT64, "long"),
            new PrimitiveType(OPTIONAL, BINARY, "long_encrypted")))));
    Builder builder = createBuilder(file, schema, cellManager, conf);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("name", (String)skjvTestData.get("name")[i]);
        g.add("provider", (String)skjvTestData.get("provider")[i]);
        if((Boolean)skjvTestData.get("optional_fields")[i]) {
          // Randomly write / withhold optional field.
          Group links = g.addGroup("Waypoints");
          Group list = links.addGroup("list");
          for(int j = 0; j < arrayElements; j+=1) {
            Group element = list.addGroup("element");
            int testDataIndex = (i+j)%numRecord;
            element.add(0, (Long)skjvTestData.get("long")[testDataIndex]);
            element.add(1, (String)skjvTestData.get("long_encrypted")[testDataIndex]);
          }
        }
        writer.write(g);
      }
    }
    return file;
  }

  private String writeArrayRepeatedLeaf(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(REQUIRED, BINARY, "name"),
      new PrimitiveType(REQUIRED, BINARY, "provider"),
      new GroupType(OPTIONAL, "Waypoints",
        new PrimitiveType(REPEATED, INT64, "long"),
        new PrimitiveType(REPEATED, BINARY, "_long_cell_encr")));
    Builder builder = createBuilder(file, schema, cellManager, conf);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("name", (String)skjvTestData.get("name")[i]);
        g.add("provider", (String)skjvTestData.get("provider")[i]);
        if((Boolean)skjvTestData.get("optional_fields")[i]) {
          // Randomly write / withhold optional field.
          Group waypoints = g.addGroup("Waypoints");
          for(int j = 0; j < arrayElements; j+=1) {
            int testDataIndex = (i+j)%numRecord;
            waypoints.add(0, (Long)skjvTestData.get("long")[testDataIndex]);
            waypoints.add(1, (String)skjvTestData.get("long_encrypted")[testDataIndex]);
          }
        }
        writer.write(g);
      }
    }
    return file;
  }

  private String writeCellEncryptedNoProvider(String file, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(REQUIRED, BINARY, "lat_encrypted"),
      new PrimitiveType(REQUIRED, INT64, "lat"));
    Builder builder = createBuilder(file, schema, null, conf);
    // Write the parquet file
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("lat_encrypted", (String)skjvTestData.get("lat_encrypted")[i]);
        g.add("lat", (Long)skjvTestData.get("lat")[i]);
        writer.write(g);
      }
    }
    return file;
  }

  private String writeCellEncryptedRequiredColumns(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(REQUIRED, BINARY, "lat_encrypted"),
      new PrimitiveType(REQUIRED, BINARY, "lat_provider"),
      new PrimitiveType(REQUIRED, INT64, "lat"),
      new GroupType(OPTIONAL, "WebLinks",
        new PrimitiveType(REPEATED, BINARY, "LinkedIn"),
        new PrimitiveType(REPEATED, BINARY, "Twitter")));
    Builder builder = createBuilder(file, schema, cellManager, conf);
    // Write the parquet file
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("lat_provider", (String)skjvTestData.get("lat_provider")[i]);
        g.add("lat", (Long)skjvTestData.get("lat")[i]);
        g.add("lat_encrypted", (String)skjvTestData.get("lat_encrypted")[i]);
        Group links = g.addGroup("WebLinks");
        links.add(0, (String)skjvTestData.get("LinkedIn")[i]);
        links.add(1, (String)skjvTestData.get("Twitter")[i]);
        writer.write(g);
      }
    }
    return file;
  }

  private String writeNullEncryptedValue(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, BINARY, "lat_encrypted"),
      new PrimitiveType(REQUIRED, BINARY, "lat_provider"),
      new PrimitiveType(OPTIONAL, INT64, "lat"),
      new GroupType(OPTIONAL, "WebLinks",
        new PrimitiveType(REPEATED, BINARY, "LinkedIn"),
        new PrimitiveType(REPEATED, BINARY, "Twitter")));
    Builder builder = createBuilder(file, schema, cellManager, conf);
    // Write the parquet file
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("lat_provider", (String)skjvTestData.get("lat_provider")[i]);
        g.add("lat", (Long)skjvTestData.get("lat")[i]);
        if((Boolean)skjvTestData.get("optional_fields")[i]) {
          // Sometimes leave out optional lat_encrypted field from written record.
          // lat_encrypted should be populated anyways appropriately by cell encryption logic.
          g.add("lat_encrypted", (String)skjvTestData.get("lat_encrypted")[i]);
        }
        Group links = g.addGroup("WebLinks");
        links.add(0, (String)skjvTestData.get("LinkedIn")[i]);
        links.add(1, (String)skjvTestData.get("Twitter")[i]);
        writer.write(g);
      }
    }
    return file;
  }

  private String writeNullOriginalValue(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, BINARY, "_lat_cell_encr"),
      new PrimitiveType(REQUIRED, BINARY, "lat_provider"),
      new PrimitiveType(OPTIONAL, INT64, "lat"),
      new GroupType(OPTIONAL, "WebLinks",
        new PrimitiveType(OPTIONAL, INT64, "long"),
        new PrimitiveType(OPTIONAL, BINARY, "_long_cell_encr")));
    Builder builder = createBuilder(file, schema, cellManager, conf);
    // Write the parquet file
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("lat_provider", (String)skjvTestData.get("lat_provider")[i]);
        // Do not include "lat" in the record for this test
        g.add("_lat_cell_encr", (String)skjvTestData.get("lat_encrypted")[i]);
        writer.write(g);
      }
    }
    return file;
  }

  private String writeSimpleEncryptColumn(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, BINARY, "lat_encrypted"),
      new PrimitiveType(REQUIRED, BINARY, "provider"),
      new PrimitiveType(OPTIONAL, INT64, "lat"),
      new PrimitiveType(OPTIONAL, INT64, "long"),
      new GroupType(OPTIONAL, "Locations",
        new PrimitiveType(OPTIONAL, BINARY, "lat_encrypted")));

    Builder builder = createBuilder(file, schema, cellManager, conf);
    // Write the parquet file
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("provider", (String)skjvTestData.get("lat_provider")[i]);
        g.add("lat", (Long)skjvTestData.get("lat")[i]);
        g.add("long", (Long)skjvTestData.get("lat")[i]);
        Group locations = g.addGroup("Locations");
        locations.add(0, (String)skjvTestData.get("lat_encrypted")[i]);
        writer.write(g);
      }
    }
    return file;
  }

  private String writeNonBinaryProviderEncryption(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, BINARY, "lat_encrypted"),
      new PrimitiveType(REQUIRED, BOOLEAN, "lat_provider"),
      new PrimitiveType(OPTIONAL, INT64, "lat"),
      new GroupType(OPTIONAL, "WebLinks",
        new PrimitiveType(REPEATED, BINARY, "LinkedIn"),
        new PrimitiveType(REPEATED, BINARY, "Twitter")));

    Builder builder = createBuilder(file, schema, cellManager, conf);
    // Write the parquet file
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("lat_provider", (Boolean) skjvTestData.get("optional_fields")[i]);
        g.add("lat", (Long)skjvTestData.get("lat")[i]);
        g.add("lat_encrypted", (String)skjvTestData.get("lat_encrypted")[i]);
        Group links = g.addGroup("WebLinks");
        links.add(0, (String)skjvTestData.get("LinkedIn")[i]);
        links.add(1, (String)skjvTestData.get("Twitter")[i]);
        writer.write(g);
      }
    }
    return file;
  }

  private String writeNonBinaryEncryptedColEncryption(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, INT64, "lat_encrypted"),
      new PrimitiveType(REQUIRED, BINARY, "lat_provider"),
      new PrimitiveType(OPTIONAL, INT64, "lat"),
      new GroupType(OPTIONAL, "WebLinks",
        new PrimitiveType(REPEATED, BINARY, "LinkedIn"),
        new PrimitiveType(REPEATED, BINARY, "Twitter")));

    Builder builder = createBuilder(file, schema, cellManager, conf);
    // Write the parquet file
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("lat_provider", (String) skjvTestData.get("lat_provider")[i]);
        g.add("lat", (Long)skjvTestData.get("lat")[i]);
        Group links = g.addGroup("WebLinks");
        links.add(0, (String)skjvTestData.get("LinkedIn")[i]);
        links.add(1, (String)skjvTestData.get("Twitter")[i]);
        writer.write(g);
      }
    }
    return file;
  }

  private String writeAllTypesEncryption(
    String file, CellManager cellManager, Configuration conf,
    BiFunction<MessageType, Integer, Group> dataFunc) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, BINARY, "provider"),
      new PrimitiveType(OPTIONAL, INT32, "int"),
      new PrimitiveType(OPTIONAL, BINARY, "int_encr"),
      new PrimitiveType(OPTIONAL, INT64, "int64"),
      new PrimitiveType(OPTIONAL, BINARY, "int64_encr"),
      new PrimitiveType(OPTIONAL, DOUBLE, "double"),
      new PrimitiveType(OPTIONAL, BINARY, "double_encr"),
      new PrimitiveType(OPTIONAL, FLOAT, "float"),
      new PrimitiveType(OPTIONAL, BINARY, "float_encr"),
      new PrimitiveType(OPTIONAL, BOOLEAN, "boolean"),
      new PrimitiveType(OPTIONAL, BINARY, "boolean_encr"),
      new PrimitiveType(OPTIONAL, BINARY, "binary"),
      new PrimitiveType(OPTIONAL, BINARY, "binary_encr"),
      new PrimitiveType(OPTIONAL, INT96, "int96"),
      new PrimitiveType(OPTIONAL, BINARY, "int96_encr"),
      new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, 12, "fixedlenbytearray"),
      new PrimitiveType(OPTIONAL, BINARY, "fixedlenbytearray_encr"));

    Builder builder = createBuilder(file, schema, cellManager, conf);
    // Write the parquet file
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        writer.write(dataFunc.apply(schema, i));
      }
    }
    return file;
  }

  private String writeAllTypesEncryption(String file, CellManager cellManager, Configuration conf) throws IOException {
    return writeAllTypesEncryption(file, cellManager, conf, (schema, i) -> {
      SimpleGroup g = new SimpleGroup(schema);
      g.add("provider", (String) skjvTestData.get("provider")[i]);
      g.add("int", (Integer)skjvTestData.get("integer")[i]);
      g.add("int64", (Long)skjvTestData.get("long")[i]);
      g.add("double", (Double)skjvTestData.get("double")[i]);
      g.add("float", (Float)skjvTestData.get("float")[i]);
      g.add("boolean", (Boolean) skjvTestData.get("optional_fields")[i]);
      g.add("binary", (String)skjvTestData.get("name")[i]);
      g.add("int96", (Binary)skjvTestData.get("binaries")[i]);
      g.add("fixedlenbytearray", (Binary)skjvTestData.get("binaries")[i]);
      return g;
    });
  }

  private String writeNullRepeatedElements(String file,
                                           CellManager cellManager,
                                           Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, BINARY, "provider"),
      new GroupType(REPEATED, "waypoints",
        new PrimitiveType(OPTIONAL, INT64, "lat"),
        new PrimitiveType(OPTIONAL, BINARY, "lat_encr")));
    Builder builder = createBuilder(file, schema, cellManager, conf);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("provider", (String)skjvTestData.get("lat_provider")[i]);

        Group waypoints = g.addGroup("waypoints");
        waypoints.add("lat", (Long)skjvTestData.get("lat")[i]);

        Group waypoints2 = g.addGroup("waypoints");
        waypoints2.add("lat_encr", (String)skjvTestData.get("lat_encrypted")[i]);

        Group waypoints3 = g.addGroup("waypoints");
        waypoints3.add("lat", (Long)skjvTestData.get("lat")[i]);

        Group waypoints4 = g.addGroup("waypoints");
        writer.write(g);
      }
    }
    return file;
  }

  private String writeCellEncryptionDictionaryEncoded(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, BINARY, "lat_encrypted"),
      new PrimitiveType(OPTIONAL, BINARY, "provider"),
      new PrimitiveType(OPTIONAL, DOUBLE, "lat"),
      new PrimitiveType(OPTIONAL, INT64, "long"),
      new PrimitiveType(OPTIONAL, BINARY, "long_encr"),
      new PrimitiveType(OPTIONAL, INT32, "int"),
      new PrimitiveType(OPTIONAL, BINARY, "int_encr"),
      new PrimitiveType(OPTIONAL, FLOAT, "float"),
      new PrimitiveType(OPTIONAL, BINARY, "float_encr"),
      new PrimitiveType(OPTIONAL, BOOLEAN, "boolean"),
      new PrimitiveType(OPTIONAL, BINARY, "boolean_encr"),
      new PrimitiveType(OPTIONAL, BINARY, "binary"),
      new PrimitiveType(OPTIONAL, BINARY, "binary_encr"));
    List<Double> doubleList = Arrays.asList(180.2313398123454, 21.2312371711234323, 60.012981928122334532);
    List<Long> longList = Arrays.asList(1802L, 212L, 600L);
    List<Integer> intList = Arrays.asList(1802, 212, 600);
    List<Float> floatList = Arrays.asList(180.231f, 21.231f, 60.012f);
    List<Boolean> boolList = Arrays.asList(false, true, false);
    List<String> binaryList = Arrays.asList("180.2", "21.2", "60.0");
    Builder builder = createBuilder(file, schema, cellManager, conf);
    // Write the parquet file
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        int nextIndex = rnd.nextInt(doubleList.size());
        if (nextIndex == 1) {
          // Only encrypt for one of the value and then filter for that value in DictionaryEncoding unit test.
          g.add("provider", providerEncryptFlag);
        } else {
          g.add("provider", "non-encrypted");
        }
        g.add("lat", doubleList.get(nextIndex));
        g.add("long", longList.get(nextIndex));
        g.add("int", intList.get(nextIndex));
        g.add("float", floatList.get(nextIndex));
        g.add("boolean", boolList.get(nextIndex));
        g.add("binary", binaryList.get(nextIndex));
        writer.write(g);
      }
    }
    return file;
  }

  private String writeHundredPercentEncrypted(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, BINARY, "lat_encrypted"),
      new PrimitiveType(OPTIONAL, BINARY, "lat_provider"),
      new PrimitiveType(OPTIONAL, INT64, "lat"));
    Builder builder = createBuilder(file, schema, cellManager, conf);
    // Write the parquet file
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("lat_provider", providerEncryptFlag);
        g.add("lat", (Long)skjvTestData.get("lat")[i]);
        g.add("lat_encrypted", (String)skjvTestData.get("lat_encrypted")[i]);
        writer.write(g);
      }
    }
    return file;
  }

  private String writePreEncryptedData(String file, CellManager cellManager,
                                       Configuration conf, boolean isInvalid) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, BINARY, "lat_encrypted"),
      new PrimitiveType(OPTIONAL, BINARY, "lat_provider"),
      new PrimitiveType(OPTIONAL, INT64, "lat"));
    Builder builder = createBuilder(file, schema, cellManager, conf);

    // Write the parquet file
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        // provider doesn't matter for pass-through mode
        g.add("lat_provider", providerEncryptFlag);
        // let original lat column be null, all cells are encrypted
        if (isInvalid) {
          g.add("lat", (Long)skjvTestData.get("lat")[i]);
        }
        Binary encryptedVal = Binary.fromConstantByteArray(cellManager
          .encryptLong((Long)skjvTestData.get("lat")[i], providerEncryptFlag));
        g.add("lat_encrypted", encryptedVal);
        writer.write(g);
      }
    }
    return file;
  }


  private String writeZeroPercentEncrypted(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, BINARY, "lat_encrypted"),
      new PrimitiveType(REQUIRED, BINARY, "lat_provider"),
      new PrimitiveType(OPTIONAL, INT64, "lat"));
    Builder builder = createBuilder(file, schema, cellManager, conf);
    // Write the parquet file
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("lat_provider", "noEncrypt");
        g.add("lat", (Long)skjvTestData.get("lat")[i]);
        g.add("lat_encrypted", (String)skjvTestData.get("lat_encrypted")[i]);
        writer.write(g);
      }
    }
    return file;
  }

  private String writeCellEncryptedFile(String file, CellManager cellManager, Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, BINARY, "lat_encrypted"),
      new PrimitiveType(REQUIRED, BINARY, "lat_provider"),
      new PrimitiveType(OPTIONAL, INT64, "lat"),
      new GroupType(OPTIONAL, "WebLinks",
        new PrimitiveType(REPEATED, BINARY, "LinkedIn"),
        new PrimitiveType(REPEATED, BINARY, "Twitter")));
    Builder builder = createBuilder(file, schema, cellManager, conf);
    // Write the parquet file
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("lat_provider", (String)skjvTestData.get("lat_provider")[i]);
        g.add("lat", (Long)skjvTestData.get("lat")[i]);
        g.add("lat_encrypted", (String)skjvTestData.get("lat_encrypted")[i]);
        Group links = g.addGroup("WebLinks");
        links.add(0, (String)skjvTestData.get("LinkedIn")[i]);
        links.add(1, (String)skjvTestData.get("Twitter")[i]);
        writer.write(g);
      }
    }
    return file;
  }

  private String writeOptionalProviderSubgroup(String file,
                                               CellManager cellManager,
                                               Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, INT64, "lat"),
      new PrimitiveType(OPTIONAL, BINARY, "lat_encr"),
      new GroupType(OPTIONAL, "datasource",
        new PrimitiveType(OPTIONAL, INT32, "id"),
        new PrimitiveType(OPTIONAL, BINARY, "provider")));
    Builder builder = createBuilder(file, schema, cellManager, conf);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("lat", (Long)skjvTestData.get("lat")[i]);
        if (i % 2 == 0) {
          Group datasource = g.addGroup("datasource");
          datasource.add("id", (Integer)skjvTestData.get("integer")[i]);
          if (i % 3 == 0) {
            datasource.add("provider", (String)skjvTestData.get("lat_provider")[i]);
          }
        }
        writer.write(g);
      }
    }
    return file;
  }

  private String writeRepeatedProvider(String file,
                                       CellManager cellManager,
                                       Configuration conf) throws IOException {
    org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("schema",
      new PrimitiveType(OPTIONAL, INT32, "trip_id"),
      new GroupType(REPEATED, "waypoints",
        new GroupType(OPTIONAL, "location",
          new PrimitiveType(OPTIONAL, DOUBLE, "lat"),
          new PrimitiveType(OPTIONAL, BINARY, "lat_encr"),
          new GroupType(OPTIONAL, "datasource",
            new PrimitiveType(OPTIONAL, BINARY, "provider")))));
    Builder builder = createBuilder(file, schema, cellManager, conf);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("trip_id", (Integer)skjvTestData.get("integer")[i]);
        Group waypoints;
        for (int j = 0; j < arrayElements; j++) {
          waypoints = g.addGroup("waypoints");
          Group location = waypoints.addGroup("location");
          location.add("lat", (Double)skjvTestData.get("double")[(i+j)%numRecord]);
          if (j != 2 && j != arrayElements-1) { // Skip provider for j=2 and last j.
            Group datasource = location.addGroup("datasource");
            datasource.add("provider", (String)skjvTestData.get("lat_provider")[(i+j)%numRecord]);
          }
        }
        writer.write(g);
      }
    }
    return file;
  }

  private String writeRepeatedProviderForDeepSubtree(String file,
                                                     CellManager cellManager,
                                                     Configuration conf) throws IOException {
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
    Builder builder = createBuilder(file, schema, cellManager, conf);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("trip_id", (Integer)skjvTestData.get("integer")[i]);
        // Array element 1
        Group array1 = g.addGroup("array");

        Group list1 = array1.addGroup("list");
        list1.add("provider", (String)skjvTestData.get("lat_provider")[i]);
        Group points1 = list1.addGroup("points");
        points1.add("lat", (Long)skjvTestData.get("lat")[i]);
        Group longlist1 = points1.addGroup("longlist");
        longlist1.add("long", (Long)skjvTestData.get("long")[(i+1)%numRecord]);
        Group longlist2 = points1.addGroup("longlist");
        longlist2.add("long", (Long)skjvTestData.get("long")[(i+2)%numRecord]);
        Group points2 = list1.addGroup("points");
        points2.add("lat", (Long)skjvTestData.get("lat")[i]);

        Group list2 = array1.addGroup("list");
        list2.add("provider", (String)skjvTestData.get("lat_provider")[(i+1)%numRecord]);
        Group points1ForList2 = list2.addGroup("points");
        points1ForList2.add("lat", (Long)skjvTestData.get("lat")[(i+3)%numRecord]);
        Group longlist1ForList2 = points1ForList2.addGroup("longlist");
        longlist1ForList2.add("long", (Long)skjvTestData.get("long")[(i+4)%numRecord]);
        Group longlist2ForList2 = points1ForList2.addGroup("longlist");
        longlist2ForList2.add("long", (Long)skjvTestData.get("long")[(i+5)%numRecord]);

        Group list3 = array1.addGroup("list");
        list3.add("provider", (String)skjvTestData.get("lat_provider")[(i+2)%numRecord]);

        Group array2 = g.addGroup("array");
        Group list4 = array2.addGroup("list");
        Group points4 = list4.addGroup("points");
        points4.add("lat", (Long)skjvTestData.get("lat")[(i+4)%numRecord]);
        Group longlist4 = points4.addGroup("longlist");
        longlist4.add("long", (Long)skjvTestData.get("long")[(i+4)%numRecord]);

        Group emptyArray = g.addGroup("array");
        writer.write(g);
      }
    }
    return file;
  }

  private void readCellEncryptedFile(String file, Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      String providerVal = (String)skjvTestData.get("lat_provider")[i];
      assertEquals(skjvTestData.get("lat_provider")[i], group.getBinary("lat_provider", 0).toStringUsingUTF8());

      if (providerVal.equals(providerEncryptFlag)) {
        // Check if column is encrypted as expected
        String encryptedVal = group.getBinary("lat_encrypted", 0).toStringUsingUTF8();
        assertTrue(checkBasicEncrypted(encryptedVal));
        assertEquals(0, group.getFieldRepetitionCount("lat"));
      } else {
        assertEquals(skjvTestData.get("lat")[i], group.getLong("lat", 0));
        assertEquals(0, group.getFieldRepetitionCount("lat_encrypted"));
      }

      Group subGroup = group.getGroup("WebLinks", 0);
      assertArrayEquals(((String)skjvTestData.get("LinkedIn")[i]).getBytes(), subGroup.getBinary("LinkedIn", 0).getBytes());
      assertArrayEquals(((String)skjvTestData.get("Twitter")[i]).getBytes(), subGroup.getBinary("Twitter", 0).getBytes());
    }
    reader.close();
  }

  private void readAllTypesEncryption(String file, Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      String providerVal = (String)skjvTestData.get("provider")[i];
      assertEquals(skjvTestData.get("provider")[i], group.getBinary("provider", 0).toStringUsingUTF8());

      if (providerVal.equals(providerEncryptFlag)) {
        // Check if columns are encrypted as expected
        // Integer
        String encryptedValInt = group.getBinary("int_encr", 0).toStringUsingUTF8();
        assertTrue(checkBasicEncrypted(encryptedValInt));
        assertEquals(0, group.getFieldRepetitionCount("int"));
        // INT64
        String encryptedValInt64 = group.getBinary("int64_encr", 0).toStringUsingUTF8();
        assertTrue(checkBasicEncrypted(encryptedValInt64));
        assertEquals(0, group.getFieldRepetitionCount("int64"));
        // Double
        String encryptedValDouble = group.getBinary("double_encr", 0).toStringUsingUTF8();
        assertTrue(checkBasicEncrypted(encryptedValDouble));
        assertEquals(0, group.getFieldRepetitionCount("double"));
        // Float
        String encryptedValFloat = group.getBinary("float_encr", 0).toStringUsingUTF8();
        assertTrue(checkBasicEncrypted(encryptedValFloat));
        assertEquals(0, group.getFieldRepetitionCount("float"));
        // Boolean
        String encryptedValBoolean = group.getBinary("boolean_encr", 0).toStringUsingUTF8();
        assertTrue(checkBasicEncrypted(encryptedValBoolean));
        assertEquals(0, group.getFieldRepetitionCount("boolean"));
        // Binary
        String encryptedValBinary = group.getBinary("binary_encr", 0).toStringUsingUTF8();
        assertTrue(checkBasicEncrypted(encryptedValBinary));
        assertEquals(0, group.getFieldRepetitionCount("binary"));
        // INT96
        String encryptedValINT96 = group.getBinary("int96_encr", 0).toStringUsingUTF8();
        assertTrue(checkBasicEncrypted(encryptedValINT96));
        assertEquals(0, group.getFieldRepetitionCount("int96"));
        // FIXED_LEN_BYTE_ARRAY
        String encryptedValFIXED_LEN_BYTE_ARRAY = group.getBinary("fixedlenbytearray_encr", 0).toStringUsingUTF8();
        assertTrue(checkBasicEncrypted(encryptedValFIXED_LEN_BYTE_ARRAY));
        assertEquals(0, group.getFieldRepetitionCount("fixedlenbytearray"));
      } else {
        // Integer
        assertEquals(skjvTestData.get("integer")[i], group.getInteger("int", 0));
        assertEquals(0, group.getFieldRepetitionCount("int_encr"));
        // INT64
        assertEquals(skjvTestData.get("long")[i], group.getLong("int64", 0));
        assertEquals(0, group.getFieldRepetitionCount("int64_encr"));
        // Double
        assertEquals(skjvTestData.get("double")[i], group.getDouble("double", 0));
        assertEquals(0, group.getFieldRepetitionCount("double_encr"));
        // Float
        assertEquals(skjvTestData.get("float")[i], group.getFloat("float", 0));
        assertEquals(0, group.getFieldRepetitionCount("float_encr"));
        // Boolean
        assertEquals(skjvTestData.get("optional_fields")[i], group.getBoolean("boolean", 0));
        assertEquals(0, group.getFieldRepetitionCount("boolean_encr"));
        // Binary
        assertEquals(skjvTestData.get("name")[i], group.getBinary("binary", 0).toStringUsingUTF8());
        assertEquals(0, group.getFieldRepetitionCount("binary_encr"));
        // INT96
        assertEquals(skjvTestData.get("binaries")[i], group.getInt96("int96", 0));
        assertEquals(0, group.getFieldRepetitionCount("int96_encr"));
        // FIXED_LEN_BYTE_ARRAY
        assertEquals(skjvTestData.get("binaries")[i], group.getBinary("fixedlenbytearray", 0));
        assertEquals(0, group.getFieldRepetitionCount("fixedlenbytearray_encr"));
      }
    }
    reader.close();
  }

  private void readNullRepeatedElements(String file, Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      String providerVal = (String)skjvTestData.get("lat_provider")[i];
      assertEquals(group.getBinary("provider", 0).toStringUsingUTF8(), providerVal);
      Group waypoints1 = group.getGroup("waypoints", 0);
      Group waypoints2 = group.getGroup("waypoints", 1);
      Group waypoints3 = group.getGroup("waypoints", 2);
      Group waypoints4 = group.getGroup("waypoints", 3);
      if (providerVal.equals(providerEncryptFlag)) {
        assertEquals(0, waypoints1.getFieldRepetitionCount("lat"));
        assertEquals(0, waypoints2.getFieldRepetitionCount("lat"));
        assertEquals(0, waypoints3.getFieldRepetitionCount("lat"));
        assertEquals(0, waypoints4.getFieldRepetitionCount("lat"));
        String encryptedVal1 = waypoints1.getBinary("lat_encr", 0).toStringUsingUTF8();
        String encryptedVal3 = waypoints3.getBinary("lat_encr", 0).toStringUsingUTF8();
        assertEquals(0, waypoints2.getFieldRepetitionCount("lat_encr"));
        assertEquals(0, waypoints4.getFieldRepetitionCount("lat_encr"));
        assertTrue(checkBasicEncrypted(encryptedVal1));
        assertTrue(checkBasicEncrypted(encryptedVal3));
      } else {
        assertEquals(skjvTestData.get("lat")[i], waypoints1.getLong("lat", 0));
        assertEquals(skjvTestData.get("lat")[i], waypoints3.getLong("lat", 0));
        assertEquals(0, waypoints1.getFieldRepetitionCount("lat_encr"));
        assertEquals(0, waypoints3.getFieldRepetitionCount("lat_encr"));
        assertEquals(0, waypoints2.getFieldRepetitionCount("lat"));
        assertEquals(0, waypoints4.getFieldRepetitionCount("lat"));
      }
    }
  }

  private void readNullOriginalFile(String file, Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertEquals(skjvTestData.get("lat_provider")[i], group.getBinary("lat_provider", 0).toStringUsingUTF8());

      assertEquals(0, group.getFieldRepetitionCount("lat"));
      assertEquals(0, group.getFieldRepetitionCount("_lat_cell_encr"));

      assertEquals(0, group.getFieldRepetitionCount("WebLinks"));
    }
    reader.close();
  }

  private void readCellEncryptedNoProvider(String file, Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertEquals(skjvTestData.get("lat")[i], group.getLong("lat", 0));
      assertEquals((skjvTestData.get("lat_encrypted")[i]).toString(), group.getBinary("lat_encrypted", 0).toStringUsingUTF8());
    }
    reader.close();
  }

  private void readCellEncryptedRequiredSubGroup(String file, Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      String latProviderVal = (String)skjvTestData.get("lat_provider")[i];
      assertEquals(skjvTestData.get("lat_provider")[i], group.getBinary("lat_provider", 0).toStringUsingUTF8());

      if (latProviderVal.equals(providerEncryptFlag)) {
        // Check if column is encrypted as expected
        String encryptedVal = group.getBinary("lat_encrypted", 0).toStringUsingUTF8();
        assertTrue(checkBasicEncrypted(encryptedVal));
        assertEquals(0, group.getFieldRepetitionCount("lat"));
      } else {
        assertEquals(skjvTestData.get("lat")[i], group.getLong("lat", 0));
        assertEquals(0, group.getFieldRepetitionCount("lat_encrypted"));
      }

      String providerVal = (String)skjvTestData.get("long_provider")[i];
      Group subGroup = group.getGroup("Locations", 0);
      if (providerVal.equals(providerEncryptFlag)) {
        String encryptedVal = subGroup.getBinary("long_encrypted", 0).toStringUsingUTF8();
        assertTrue(checkBasicEncrypted(encryptedVal));
        assertEquals(0, subGroup.getFieldRepetitionCount("long"));
      } else {
        assertEquals(0, subGroup.getFieldRepetitionCount("long_encrypted"));
        assertEquals((skjvTestData.get("long")[i]), subGroup.getLong("long", 0));
      }
      assertEquals(providerVal, subGroup.getBinary("long_provider", 0).toStringUsingUTF8());
    }
    reader.close();
  }

  private void readCellEncryptedOptionalSubGroup(String file, Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertEquals(skjvTestData.get("name")[i], group.getBinary("name", 0).toStringUsingUTF8());
      assertEquals(skjvTestData.get("lat")[i], group.getLong("lat", 0));
      assertEquals(skjvTestData.get("nickname")[i], group.getBinary("nickname", 0).toStringUsingUTF8());

      String providerVal = (String)skjvTestData.get("long_provider")[i];
      // Check if optional field was written
      if ((Boolean)skjvTestData.get("optional_fields")[i]) {
        Group subGroup = group.getGroup("Locations", 0);
        if (providerVal.equals(providerEncryptFlag)) {
          String encryptedVal = subGroup.getBinary("long_encrypted", 0).toStringUsingUTF8();
          assertTrue(checkBasicEncrypted(encryptedVal));
          assertEquals(0, subGroup.getFieldRepetitionCount("long"));
        } else {
          assertEquals(0, subGroup.getFieldRepetitionCount("long_encrypted"));
          assertEquals(skjvTestData.get("long")[i], subGroup.getLong("long", 0));
        }
        assertEquals(providerVal, subGroup.getBinary("long_provider", 0).toStringUsingUTF8());
      }
    }
    reader.close();
  }

  private void readCellEncryptedDiffProviderLevel(String file, Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertEquals(skjvTestData.get("name")[i], group.getBinary("name", 0).toStringUsingUTF8());
      assertEquals((String)skjvTestData.get("long_provider")[i], group.getBinary("long_provider", 0).toStringUsingUTF8());

      String providerVal = (String)skjvTestData.get("long_provider")[i];
      String latProviderVal = (String)skjvTestData.get("lat_provider")[i];
      // Check if optional field was written
      if ((Boolean)skjvTestData.get("optional_fields")[i]) {
        Group subGroup = group.getGroup("Locations", 0);
        if (providerVal.equals(providerEncryptFlag)) {
          String encryptedVal = subGroup.getBinary("long_encrypted", 0).toStringUsingUTF8();
          assertTrue(checkBasicEncrypted(encryptedVal));
          assertEquals(0, subGroup.getFieldRepetitionCount("long"));
        } else {
          assertEquals(skjvTestData.get("long")[i], subGroup.getLong("long", 0));
          assertEquals(0, subGroup.getFieldRepetitionCount("long_encrypted"));
        }
        assertEquals(skjvTestData.get("lat_provider")[i], subGroup.getBinary("lat_provider", 0).toStringUsingUTF8());
      }
      if ((Boolean)skjvTestData.get("optional_fields")[i] && latProviderVal.equals(providerEncryptFlag)) {
        assertTrue(checkBasicEncrypted(group.getBinary("lat_encrypted", 0).toStringUsingUTF8()));
        assertEquals(0, group.getFieldRepetitionCount("lat"));
      } else {
        assertEquals(skjvTestData.get("lat")[i], group.getLong("lat", 0));
        assertEquals(0, group.getFieldRepetitionCount("lat_encrypted"));
      }
    }
    reader.close();
  }

  private void readSameProviderTwoColumns(String file, Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertEquals(skjvTestData.get("name")[i], group.getBinary("name", 0).toStringUsingUTF8());

      String providerVal = (String)skjvTestData.get("provider")[i];
      // Check if optional field was written
      if ((Boolean)skjvTestData.get("optional_fields")[i]) {
        Group subGroup = group.getGroup("Locations", 0);
        assertEquals(skjvTestData.get("provider")[i], subGroup.getBinary("provider", 0).toStringUsingUTF8());
        if (providerVal.equals(providerEncryptFlag)) {
          String encryptedVal = subGroup.getBinary("long_encrypted", 0).toStringUsingUTF8();
          assertTrue(checkBasicEncrypted(encryptedVal));
          assertEquals(0, subGroup.getFieldRepetitionCount("long"));
        } else {
          assertEquals(skjvTestData.get("long")[i], subGroup.getLong("long", 0));
          assertEquals(0, subGroup.getFieldRepetitionCount("long_encrypted"));
        }

      }
      if ((Boolean)skjvTestData.get("optional_fields")[i] && providerVal.equals(providerEncryptFlag)) {
        assertTrue(checkBasicEncrypted(group.getBinary("lat_encrypted", 0).toStringUsingUTF8()));
        assertEquals(0, group.getFieldRepetitionCount("lat"));
      } else {
        assertEquals(skjvTestData.get("lat")[i], group.getLong("lat", 0));
        assertEquals(0, group.getFieldRepetitionCount("lat_encrypted"));
      }
    }
    reader.close();
  }

  private void readEncryptionManySubGroups(String file, Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertEquals(skjvTestData.get("name")[i], group.getBinary("name", 0).toStringUsingUTF8());
      Group location1 = group.getGroup("Locations", 0);
      Group location2 = group.getGroup("Locations", 1);
      assertEquals(0, location1.getFieldRepetitionCount("long"));
      assertTrue(checkBasicEncrypted(location1.getBinary("_long_cell_encr", 0).toStringUsingUTF8()));
      assertEquals(0, location2.getFieldRepetitionCount("long"));
      assertTrue(checkBasicEncrypted(location2.getBinary("_long_cell_encr", 0).toStringUsingUTF8()));
      Group waypoints = location1.getGroup("waypoints", 0);
      assertEquals(0, waypoints.getFieldRepetitionCount("lat"));
      assertTrue(checkBasicEncrypted(waypoints.getBinary("_lat_cell_encr", 0).toStringUsingUTF8()));
      Group pois = waypoints.getGroup("pois", 0);
      assertEquals(0, pois.getFieldRepetitionCount("placeid"));
      assertTrue(checkBasicEncrypted(pois.getBinary("_placeid_cell_encr", 0).toStringUsingUTF8()));
    }
    reader.close();
  }

  private void readArrayEncrypted(String file, Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertEquals(skjvTestData.get("name")[i], group.getBinary("name", 0).toStringUsingUTF8());
      assertEquals(skjvTestData.get("provider")[i], group.getBinary("provider", 0).toStringUsingUTF8());
      String providerVal = (String)skjvTestData.get("provider")[i];

      if ((Boolean)skjvTestData.get("optional_fields")[i]) {
        Group waypointsGroup = group.getGroup("Waypoints", 0);
        for (int j = 0; j < arrayElements; j++) {
          int testDataIndex = (i+j)%numRecord;
          Group list = waypointsGroup.getGroup("list", j);
          Group element = list.getGroup("element", 0);
          if (providerVal.equals(providerEncryptFlag)) {
            String encryptedVal = element.getBinary("long_encrypted", 0).toStringUsingUTF8();
            assertTrue(checkBasicEncrypted(encryptedVal));
            assertEquals(0, element.getFieldRepetitionCount("long"));
          } else {
            assertEquals(0,
              element.getFieldRepetitionCount("long_encrypted"));
            assertEquals(skjvTestData.get("long")[testDataIndex], element.getLong("long", 0));
          }
        }
      }
    }
    reader.close();
  }

  private void readArrayEncryptedRepeatedLeaf(String file, Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertEquals(skjvTestData.get("name")[i], group.getBinary("name", 0).toStringUsingUTF8());
      assertEquals(skjvTestData.get("provider")[i], group.getBinary("provider", 0).toStringUsingUTF8());
      String providerVal = (String)skjvTestData.get("provider")[i];

      if ((Boolean)skjvTestData.get("optional_fields")[i]) {
        Group waypointsGroup = group.getGroup("Waypoints", 0);
        for(int j = 0; j < arrayElements; j+=1) {
          int testDataIndex = (i+j)%numRecord;
          if (providerVal.equals(providerEncryptFlag)) {
            String encryptedVal = waypointsGroup.getBinary("_long_cell_encr", 0).toStringUsingUTF8();
            assertTrue(checkBasicEncrypted(encryptedVal));
            assertEquals(0, waypointsGroup.getFieldRepetitionCount("long"));
          } else {
            assertEquals(skjvTestData.get("long")[testDataIndex], waypointsGroup.getLong("long", j));
            assertEquals(0, waypointsGroup.getFieldRepetitionCount("_long_cell_encr"));
          }
        }
      }
    }
    reader.close();
  }

  private void readOptionalProviderSubgroup(String file,
                                            Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      String providerVal = "";
      if (i % 2 == 0  && i % 3 == 0) {
        providerVal = (String)skjvTestData.get("lat_provider")[i];
      }
      if (providerVal.equals(providerEncryptFlag)) {
        String encryptedVal = group.getBinary("lat_encr", 0).toStringUsingUTF8();
        assertTrue(checkBasicEncrypted(encryptedVal));
        assertEquals(0, group.getFieldRepetitionCount("lat"));
      } else {
        assertEquals(skjvTestData.get("lat")[i], group.getLong("lat", 0));
        assertEquals(0, group.getFieldRepetitionCount("lat_encr"));
      }
    }
    reader.close();
  }

  private void readRepeatedProvider(String file, Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertEquals(skjvTestData.get("integer")[i], group.getInteger("trip_id", 0));
      Group waypoints;
      for (int j = 0; j < arrayElements; j++) {
        waypoints = group.getGroup("waypoints", j);
        int testDataIndex = (i+j)%numRecord;
        Group location = waypoints.getGroup("location", 0);
        if (j != 2 && j != arrayElements-1) {
          // Provider is not null
          Group datasource = location.getGroup("datasource", 0);
          String providerVal = datasource.getBinary("provider", 0).toStringUsingUTF8();

          if (providerVal.equals(providerEncryptFlag)) {
            String encryptedLat = location.getBinary("lat_encr", 0).toStringUsingUTF8();
            assertTrue(checkBasicEncrypted(encryptedLat));
            assertEquals(0, location.getFieldRepetitionCount("lat"));
          } else {
            // provider is not encrypt provider
            assertEquals(skjvTestData.get("double")[testDataIndex], location.getDouble("lat", 0));
            assertEquals(0, location.getFieldRepetitionCount("lat_encr"));
          }
        } else {
          // Provider is null
          assertEquals(skjvTestData.get("double")[testDataIndex], location.getDouble("lat", 0));
          assertEquals(0, location.getFieldRepetitionCount("lat_encr"));
        }
      }
    }
    reader.close();
  }

  private void readRepeatedProviderForDeepSubtree(String file, Configuration conf) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertEquals(skjvTestData.get("integer")[i], group.getInteger("trip_id", 0));
      Group array1 = group.getGroup("array", 0);
      Group list1 = array1.getGroup("list", 0);
      String list1Provider = list1.getString("provider", 0);
      assertEquals(list1Provider, skjvTestData.get("lat_provider")[i]);

      Group points1 = list1.getGroup("points", 0);
      Group longlist1 = points1.getGroup("longlist", 0);
      Group longlist2 = points1.getGroup("longlist", 1);
      Group points2 = list1.getGroup("points", 1);
      if (list1Provider.equals(providerEncryptFlag)) {
        assertEquals(0, points1.getFieldRepetitionCount("lat"));
        assertEquals(0, longlist1.getFieldRepetitionCount("long"));
        assertEquals(0, longlist2.getFieldRepetitionCount("long"));
        assertEquals(0, points2.getFieldRepetitionCount("lat"));
        assertTrue(checkBasicEncrypted(points1.getBinary("_lat_cell_encr", 0).toStringUsingUTF8()));
        assertTrue(checkBasicEncrypted(longlist1.getBinary("_long_cell_encr", 0).toStringUsingUTF8()));
        assertTrue(checkBasicEncrypted(longlist2.getBinary("_long_cell_encr", 0).toStringUsingUTF8()));
        assertTrue(checkBasicEncrypted(points2.getBinary("_lat_cell_encr", 0).toStringUsingUTF8()));
      } else {
        assertEquals(0, points1.getFieldRepetitionCount("_lat_cell_encr"));
        assertEquals(0, longlist1.getFieldRepetitionCount("_long_cell_encr"));
        assertEquals(0, longlist2.getFieldRepetitionCount("_long_cell_encr"));
        assertEquals(0, points2.getFieldRepetitionCount("_lat_cell_encr"));
        assertEquals(skjvTestData.get("lat")[i], points1.getLong("lat", 0));
        assertEquals(skjvTestData.get("long")[(i+1)%numRecord], longlist1.getLong("long", 0));
        assertEquals(skjvTestData.get("long")[(i+2)%numRecord], longlist2.getLong("long", 0));
        assertEquals(skjvTestData.get("lat")[i], points2.getLong("lat", 0));
      }

      Group list2 = array1.getGroup("list", 1);
      String list2Provider = list2.getString("provider", 0);
      assertEquals(list2Provider, skjvTestData.get("lat_provider")[(i+1)%numRecord]);
      Group points1ForList2 = list2.getGroup("points", 0);
      Group longlist1ForList2 = points1ForList2.getGroup("longlist", 0);
      Group longlist2ForList2 = points1ForList2.getGroup("longlist", 1);
      if (list2Provider.equals(providerEncryptFlag)) {
        assertEquals(0, points1ForList2.getFieldRepetitionCount("lat"));
        assertEquals(0, longlist1ForList2.getFieldRepetitionCount("long"));
        assertEquals(0, longlist2ForList2.getFieldRepetitionCount("long"));
        assertTrue(checkBasicEncrypted(points1ForList2
          .getBinary("_lat_cell_encr", 0).toStringUsingUTF8()));
        assertTrue(checkBasicEncrypted(longlist1ForList2
          .getBinary("_long_cell_encr", 0).toStringUsingUTF8()));
        assertTrue(checkBasicEncrypted(longlist2ForList2
          .getBinary("_long_cell_encr", 0).toStringUsingUTF8()));
      } else {
        assertEquals(0, points1ForList2.getFieldRepetitionCount("_lat_cell_encr"));
        assertEquals(0, longlist1ForList2.getFieldRepetitionCount("_long_cell_encr"));
        assertEquals(0, longlist2ForList2.getFieldRepetitionCount("_long_cell_encr"));
        assertEquals(skjvTestData.get("lat")[(i+3)%numRecord], points1ForList2.getLong("lat", 0));
        assertEquals(skjvTestData.get("long")[(i+4)%numRecord], longlist1ForList2.getLong("long", 0));
        assertEquals(skjvTestData.get("long")[(i+5)%numRecord], longlist2ForList2.getLong("long", 0));
      }

      Group list3 = array1.getGroup("list", 2);
      String list3Provider = list3.getString("provider", 0);
      assertEquals(list3Provider, skjvTestData.get("lat_provider")[(i+2)%numRecord]);
      assertEquals(0, list3.getFieldRepetitionCount("points"));

      Group array2 = group.getGroup("array", 1);
      Group list4 = array2.getGroup("list", 0);
      assertEquals(0, list4.getFieldRepetitionCount("provider"));
      Group points4 = list4.getGroup("points", 0);
      Group longlist4 = points4.getGroup("longlist", 0);
      // Provider for list4 is null, so lat/lng should be unencrypted.
      assertEquals(0, points4.getFieldRepetitionCount("_lat_cell_encr"));
      assertEquals(0, longlist4.getFieldRepetitionCount("_long_cell_encr"));
      assertEquals(skjvTestData.get("lat")[(i+4)%numRecord], points4.getLong("lat", 0));
      assertEquals(skjvTestData.get("long")[(i+4)%numRecord], longlist4.getLong("long", 0));

      Group emptyArray = group.getGroup("array", 2);
      assertEquals(0, emptyArray.getFieldRepetitionCount("list"));
    }
    reader.close();
  }

  private boolean checkBasicEncrypted(String value) {
    boolean encrypted = value.startsWith(BasicCellManager.BASIC_ENCRYPT_PREFIX);
    if (!encrypted) {
      LOG.error("Value is not basic cell encrypted = " + value);
    }
    return encrypted;
  }

  @Test
  public void testCellEncryptionBasic() throws Exception {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"}, new String[]{"lat_encrypted"}, new String[]{"lat_provider"});
    writeCellEncryptedFile(file, cellManager, conf);
    readCellEncryptedFile(file, conf);
  }

  @Test
  public void testCellEncryptionRequiredSubGroup() throws Exception {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"},
      new String[]{"lat_encrypted"},
      new String[]{"lat_provider"},
      new String[]{"Locations", "long"},
      new String[]{"Locations", "long_encrypted"},
      new String[]{"Locations", "long_provider"});
    writeCellEncryptedRequiredSubGroup(file, cellManager, conf);
    readCellEncryptedRequiredSubGroup(file, conf);
  }

  @Test
  public void testCellEncryptionOptionalSubGroup() throws Exception {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"Locations", "long"},
      new String[]{"Locations", "long_encrypted"},
      new String[]{"Locations", "long_provider"});
    writeCellEncryptedOptionalSubGroup(file, cellManager, conf);
    readCellEncryptedOptionalSubGroup(file, conf);
  }

  @Test
  public void testCellEncryptionNoProvider() throws Exception {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    writeCellEncryptedNoProvider(file, conf);
    readCellEncryptedNoProvider(file, conf);
  }

  @Test
  public void testCellEncryptionDiffProviderLevel() throws Exception {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"},
      new String[]{"lat_encrypted"},
      new String[]{"Locations", "lat_provider"},
      new String[]{"Locations", "long"},
      new String[]{"Locations", "long_encrypted"},
      new String[]{"long_provider"});
    writeCellEncryptedDiffProviderLevel(file, cellManager, conf);
    readCellEncryptedDiffProviderLevel(file, conf);
  }

  @Test
  public void testCellEncryptionSameProviderTwoColumns() throws Exception {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"},
      new String[]{"lat_encrypted"},
      new String[]{"Locations", "provider"},
      new String[]{"Locations", "long"},
      new String[]{"Locations", "long_encrypted"},
      new String[]{"Locations", "provider"});
    writeSameProviderTwoColumns(file, cellManager, conf);
    readSameProviderTwoColumns(file, conf);
  }

  @Test
  public void testCellEncryptedArray() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"Waypoints", "list", "element", "long"},
      new String[]{"Waypoints", "list", "element", "long_encrypted"},
      new String[]{"provider"});
    writeArrayEncrypted(file, cellManager, conf);
    readArrayEncrypted(file, conf);
  }

  @Test
  public void testEncryptedArrayRepeatedLeaf() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    LOG.warn("file " + file);
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"Waypoints", "long"},
      new String[]{"Waypoints", "_long_cell_encr"},
      new String[]{"provider"});
    writeArrayRepeatedLeaf(file, cellManager, conf);
    readArrayEncryptedRepeatedLeaf(file, conf);
  }

  @Test(expected = BadConfigurationException.class)
  public void testExternalEncryptionPluginNotExists() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    conf.set("parquet.crypto.cell.manager.factory.class", "org.apache.parquet.crypto.CryptoCellManagerFactory");
    writeCellEncryptedFile(file, null, conf);
  }

  @Test
  public void testEncryptionPluginNotSetAndNotNeeded() throws IOException {
    // Cover case where we don't set any cell encrypt plugin and we don't need it.
    // Test should not throw an exception since the plugin is not needed.
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    writeCellEncryptedNoProvider(file, conf);
    readCellEncryptedNoProvider(file, conf);
  }

  @Test(expected = IllegalStateException.class)
  public void testTryEncryptRequiredColumns() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"}, new String[]{"lat_encrypted"}, new String[]{"lat_provider"});
    writeCellEncryptedRequiredColumns(file, cellManager, conf);
  }

  @Test(expected = IllegalStateException.class)
  public void testNonExistingProviderConfigured() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"}, new String[]{"lat_encrypted"}, new String[]{"lat_provider_fake"});
    writeCellEncryptedFile(file, cellManager, conf);
  }

  @Test(expected = IllegalStateException.class)
  public void testNonExistingEncrColConfigured() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"}, new String[]{"lat_encrypted_fake"}, new String[]{"lat_provider"});
    writeCellEncryptedFile(file, cellManager, conf);
  }

  @Test(expected = IllegalStateException.class)
  public void testOriginalEncryptColsNotSiblings() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"}, new String[]{"Locations", "lat_encrypted"}, new String[]{"provider"});
    writeSimpleEncryptColumn(file, cellManager, conf);
  }

  @Test(expected = IllegalStateException.class)
  public void testEncryptedColTwiceSpecified() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"}, new String[]{"lat_encrypted"}, new String[]{"provider"},
      new String[]{"long"}, new String[]{"lat_encrypted"}, new String[]{"provider"});
    writeSimpleEncryptColumn(file, cellManager, conf);
  }

  @Test
  public void testWriteNullEncryptedValue() throws IOException {
    // Test writing null for encrypted column sometimes
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"}, new String[]{"lat_encrypted"}, new String[]{"lat_provider"});
    writeNullEncryptedValue(file, cellManager, conf);
    readCellEncryptedFile(file, conf);
  }

  @Test
  public void testWriteNullOriginalValue() throws IOException {
    // Test writing null for original column sometimes
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"},
      new String[]{"_lat_cell_encr"},
      new String[]{"lat_provider"},
      new String[]{"WebLinks", "long"},
      new String[]{"WebLinks", "_long_cell_encr"},
      new String[]{"lat_provider"});
    writeNullOriginalValue(file, cellManager, conf);
    readNullOriginalFile(file, conf);
  }

  @Test
  public void testManySubGroups() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"Locations", "long"},
      new String[]{"Locations", "_long_cell_encr"},
      new String[]{"provider"},
      new String[]{"Locations", "waypoints", "lat"},
      new String[]{"Locations", "waypoints", "_lat_cell_encr"},
      new String[]{"provider"},
      new String[]{"Locations", "waypoints", "pois", "placeid"},
      new String[]{"Locations", "waypoints", "pois", "_placeid_cell_encr"},
      new String[]{"provider"});

    writeEncryptionManySubgroups(file, cellManager, conf);
    readEncryptionManySubGroups(file, conf);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testNonBinaryProvider() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"}, new String[]{"lat_encrypted"}, new String[]{"lat_provider"});
    writeNonBinaryProviderEncryption(file, cellManager, conf);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testNonBinaryEncryptedColumn() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"}, new String[]{"lat_encrypted"}, new String[]{"lat_provider"});
    writeNonBinaryEncryptedColEncryption(file, cellManager, conf);
  }

  @Test
  public void testAllTypesEncryption() throws IOException {
    // Test encryption for other original data types
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"int"}, new String[]{"int_encr"}, new String[]{"provider"},
      new String[]{"int64"}, new String[]{"int64_encr"}, new String[]{"provider"},
      new String[]{"double"}, new String[]{"double_encr"}, new String[]{"provider"},
      new String[]{"float"}, new String[]{"float_encr"}, new String[]{"provider"},
      new String[]{"boolean"}, new String[]{"boolean_encr"}, new String[]{"provider"},
      new String[]{"binary"}, new String[]{"binary_encr"}, new String[]{"provider"},
      new String[]{"int96"}, new String[]{"int96_encr"}, new String[]{"provider"},
      new String[]{"fixedlenbytearray"}, new String[]{"fixedlenbytearray_encr"}, new String[]{"provider"});

    writeAllTypesEncryption(file, cellManager, conf);
    readAllTypesEncryption(file, conf);

    // Test pass_through mode with all types (not about correctness, just to exercise all types)
    try (ParquetReader<Group> reader =
           ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build()) {
      when(cellManager.isPassThroughModeEnabled()).thenReturn(true);

      String file2 = createTempFile("test2");
      writeAllTypesEncryption(file2, cellManager, conf, (schema, i) -> {
        try {
          return reader.read();
        }
        catch (IOException e) {
          Assert.fail(e.getMessage());
          return null;
        }
      });
    }
  }

  @Test
  public void testNullRepeatedElements() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"waypoints", "lat"},
      new String[]{"waypoints", "lat_encr"},
      new String[]{"provider"});

    writeNullRepeatedElements(file, cellManager, conf);
    readNullRepeatedElements(file, conf);
  }

  @Test
  public void testOptionalSubgroupProvider() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"}, new String[]{"lat_encr"}, new String[]{"datasource", "provider"});
    writeOptionalProviderSubgroup(file, cellManager, conf);
    readOptionalProviderSubgroup(file, conf);
  }

  @Test
  public void testRepeatedProvider() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"waypoints", "location", "lat"},
      new String[]{"waypoints", "location", "lat_encr"},
      new String[]{"waypoints", "location", "datasource", "provider"});
    writeRepeatedProvider(file, cellManager, conf);
    readRepeatedProvider(file, conf);
  }

  @Test
  public void testRepeatedProviderForDeepSubtree() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"array", "list", "points", "lat"},
      new String[]{"array", "list", "points", "_lat_cell_encr"},
      new String[]{"array", "list", "provider"},
      new String[]{"array", "list", "points", "longlist", "long"},
      new String[]{"array", "list", "points", "longlist", "_long_cell_encr"},
      new String[]{"array", "list", "provider"});
    writeRepeatedProviderForDeepSubtree(file, cellManager, conf);
    readRepeatedProviderForDeepSubtree(file, conf);
  }

  @Test
  public void testFilteringStats() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"}, new String[]{"lat_encrypted"}, new String[]{"lat_provider"});
    writeHundredPercentEncrypted(file, cellManager, conf);
    long filterGT = 10l;
    long filterLT = 18l;
    FilterPredicate pred = FilterApi.and(
      FilterApi.gt(FilterApi.longColumn("lat"), filterGT),
      FilterApi.lt(FilterApi.longColumn("lat"), filterLT));
    FilterCompat.Filter filter = FilterCompat.get(pred);

    ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), new Path(file))
      .withConf(conf).withCellManager(cellManager);

    int totalFilterValues = 0;
    // Read parquet file without filter and count the rows we would expect to be filtered
    try (ParquetReader<Group> reader = builder.build()) {
      Group group = reader.read();
      while (group != null) {
        long latVal = group.getLong("lat", 0);
        if (latVal > filterGT && latVal < filterLT) {
          totalFilterValues += 1;
        }
        group = reader.read();
      }
    }

    // Read same file withFilter and throw assertion error if different number of values are filtered.
    try (ParquetReader<Group> reader = builder.withFilter(filter).build()) {
      Group group = reader.read();
      int i = 0;
      while (group != null) {
        i += 1;
        long latVal = group.getLong("lat", 0);
        assertTrue(latVal > filterGT && latVal < filterLT);
        group = reader.read();
      }
      assertEquals(totalFilterValues, i);
    }

    // Test non cell encrypted filtering has no regressions
    String fileNonEncrypted = createTempFile("testNonEncrypted");
    writeZeroPercentEncrypted(fileNonEncrypted, cellManager, conf);
    ParquetReader.Builder<Group> builder2 = ParquetReader.builder(new GroupReadSupport(), new Path(fileNonEncrypted))
      .withConf(conf).withCellManager(cellManager);
    try (ParquetReader<Group> reader = builder2.withFilter(filter).build()) {
      Group group = reader.read();
      int i = 0;
      while (group != null) {
        i += 1;
        long latVal = group.getLong("lat", 0);
        assertTrue(latVal > filterGT && latVal < filterLT);
        group = reader.read();
      }
      assertEquals(totalFilterValues, i);
    }
  }

  @Test
  public void testFilteringNullValues() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"},
      new String[]{"_lat_cell_encr"},
      new String[]{"lat_provider"},
      new String[]{"WebLinks", "long"},
      new String[]{"WebLinks", "_long_cell_encr"},
      new String[]{"lat_provider"});
    writeNullOriginalValue(file, cellManager, conf);
    long filterGT = 10l;
    long filterLT = 18l;
    FilterPredicate pred = FilterApi.and(
      FilterApi.gt(FilterApi.longColumn("lat"), filterGT),
      FilterApi.lt(FilterApi.longColumn("lat"), filterLT));
    FilterCompat.Filter filter = FilterCompat.get(pred);

    ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), new Path(file))
      .withConf(conf).withCellManager(cellManager).withFilter(filter);
    // We wrote null for every lat value so this filtered read should return no results.
    int expectedFilteredLatCount = 0;
    try (ParquetReader<Group> reader = builder.build()) {
      Group group = reader.read();
      int i = 0;
      while (group != null) {
        i += 1;
        long latVal = group.getLong("lat", 0);
        assertTrue(latVal > filterGT && latVal < filterLT);
        group = reader.read();
      }
      assertEquals(expectedFilteredLatCount, i);
    }
  }

  @Test
  public void testCellEncryptedDictionaryEncoding() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"lat"}, new String[]{"lat_encrypted"}, new String[]{"provider"},
      new String[]{"long"}, new String[]{"long_encr"}, new String[]{"provider"},
      new String[]{"int"}, new String[]{"int_encr"}, new String[]{"provider"},
      new String[]{"float"}, new String[]{"float_encr"}, new String[]{"provider"},
      new String[]{"boolean"}, new String[]{"boolean_encr"}, new String[]{"provider"},
      new String[]{"binary"}, new String[]{"binary_encr"}, new String[]{"provider"});
    writeCellEncryptionDictionaryEncoded(file, cellManager, conf);

    ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), new Path(file))
      .withConf(conf).withCellManager(cellManager);

    int expectedCount = 0;
    // Read without filter to count expected number of filtered values.
    try (ParquetReader<Group> reader = builder.build()) {
      Group group = reader.read();
      while (group != null) {
        String providerVal = group.getBinary("provider", 0).toStringUsingUTF8();
        if (providerVal.equals(providerEncryptFlag)) {
          expectedCount += 1;
        }
        group = reader.read();
      }
    }

    double filterDoubleEquals = 21.2312371711234323d;
    long filterLongEquals = 212L;
    int filterIntegerEquals = 212;
    float filterFloatEquals = 21.231f;
    boolean filterBoolEquals = true;
    String filterBinaryEquals = "21.2";

    // Cause dictionary page based filtering to be used.
    FilterPredicate doublePred = FilterApi.eq(FilterApi.doubleColumn("lat"), filterDoubleEquals);
    FilterPredicate longPred = FilterApi.eq(FilterApi.longColumn("long"), filterLongEquals);
    FilterPredicate integerPred = FilterApi.eq(FilterApi.intColumn("int"), filterIntegerEquals);
    FilterPredicate floatPred = FilterApi.eq(FilterApi.floatColumn("float"), filterFloatEquals);
    // Note: no dictionary pages for booleans
    FilterPredicate booleanPred = FilterApi.eq(FilterApi.booleanColumn("boolean"), filterBoolEquals);
    FilterPredicate binaryPred = FilterApi.eq(FilterApi.binaryColumn("binary"), Binary.fromString(filterBinaryEquals));

    validateFilteredCount(builder, doublePred, expectedCount);
    validateFilteredCount(builder, longPred, expectedCount);
    validateFilteredCount(builder, integerPred, expectedCount);
    validateFilteredCount(builder, floatPred, expectedCount);
    validateFilteredCount(builder, booleanPred, expectedCount);
    validateFilteredCount(builder, binaryPred, expectedCount);
  }

  @Test
  public void testPassThroughMode() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    // Pass through mode can skip provider column look up - so we only mock original / encrypted cols.
    addOriginalEncryptedToMockCellManager(cellManager,
      new String[]{"lat"}, new String[]{"lat_encrypted"});
    when(cellManager.isPassThroughModeEnabled()).thenReturn(true);

    writePreEncryptedData(file, cellManager, conf, false);
    long filterGT = 10l;
    long filterLT = 18l;
    FilterPredicate pred = FilterApi.and(
      FilterApi.gt(FilterApi.longColumn("lat"), filterGT),
      FilterApi.lt(FilterApi.longColumn("lat"), filterLT));

    ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), new Path(file))
      .withConf(conf).withCellManager(cellManager);

    int totalFilterValues = 0;
    // Read parquet file without filter and count the rows we would expect to be filtered
    try (ParquetReader<Group> reader = builder.build()) {
      Group group = reader.read();
      while (group != null) {
        long latVal = group.getLong("lat", 0);
        if (latVal > filterGT && latVal < filterLT) {
          totalFilterValues += 1;
        }
        group = reader.read();
      }
    }

    // Read same file withFilter and throw assertion error if different number of values are filtered.
    validateFilteredCount(builder, pred, totalFilterValues);
  }

  @Test(expected = IllegalStateException.class)
  public void testPassThroughInvalidRecord() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addOriginalEncryptedToMockCellManager(cellManager,
      new String[]{"lat"}, new String[]{"lat_encrypted"});
    when(cellManager.isPassThroughModeEnabled()).thenReturn(true);

    writePreEncryptedData(file, cellManager, conf, true);
  }

  @Test
  public void testPassThroughManySubGroups() throws IOException {
    Configuration conf = new Configuration();
    String file = createTempFile("test");
    CellManager cellManager = spy(BasicCellManager.class);
    addMappingToMockCellManager(cellManager,
      new String[]{"Locations", "long"},
      new String[]{"Locations", "_long_cell_encr"},
      new String[]{"provider"},
      new String[]{"Locations", "waypoints", "lat"},
      new String[]{"Locations", "waypoints", "_lat_cell_encr"},
      new String[]{"provider"},
      new String[]{"Locations", "waypoints", "pois", "placeid"},
      new String[]{"Locations", "waypoints", "pois", "_placeid_cell_encr"},
      new String[]{"provider"});

    writeManyNonRepeatedSubgroups(file, cellManager, conf);

    // Step 2: Read without cellEncryption enabled as Group
    // Step 3: Immediately write to writer with passThroughMode on.
    when(cellManager.isPassThroughModeEnabled()).thenReturn(true);
    ParquetReader.Builder<Group> readBuilder = ParquetReader.builder(new GroupReadSupport(), new Path(file))
      .withConf(conf);
    String writeThruFile = createTempFile("writeThruTest");
    Builder writeBuilder = createBuilder(writeThruFile, getManySubGroupSchema(), cellManager, conf);

    try (ParquetReader<Group> reader = readBuilder.build();
         ParquetWriter<Group> writer = writeBuilder.build()) {
      Group group = reader.read();
      while (group != null) {
        writer.write(group);
        group = reader.read();
      }
    }

    ParquetReader.Builder<Group> readPassBuilder = ParquetReader.builder(new GroupReadSupport(), new Path(writeThruFile))
      .withConf(conf).withCellManager(cellManager);
    long filterGT = 10l;
    long filterLT = 18l;

    // Step 4: Read the data manually no filter and count long / lat / placeid within range
    int totalFilterValues = 0;
    try (ParquetReader<Group> reader = readPassBuilder.build()) {
      Group group = reader.read();
      while (group != null) {
        Long placeIdVal = null;
        for (int i = 0; i < group.getFieldRepetitionCount("Locations"); i++) {
          Group location = group.getGroup("Locations", 0);
          if (location.getFieldRepetitionCount("waypoints") != 0) {
            Group waypoints = location.getGroup("waypoints", 0);
            if (waypoints.getFieldRepetitionCount("pois") != 0) {
              Group pois = waypoints.getGroup("pois", 0);
              if (pois.getFieldRepetitionCount("placeid") != 0) {
                placeIdVal = pois.getLong("placeid", 0);
              }
            }
          }
        }

        if (placeIdVal != null && placeIdVal > filterGT && placeIdVal < filterLT) {
          totalFilterValues += 1;
        }
        group = reader.read();
      }
    }

    // Step 5: Read the data with filter and confirm it matches manual count
    FilterPredicate pred = FilterApi.and(
      FilterApi.gt(FilterApi.longColumn("Locations.waypoints.pois.placeid"), filterGT),
      FilterApi.lt(FilterApi.longColumn("Locations.waypoints.pois.placeid"), filterLT));

    validateFilteredCount(readPassBuilder, pred, totalFilterValues);
  }

  private static void validateFilteredCount(ParquetReader.Builder<Group> builder,
                                            FilterPredicate pred, int expectedCount) throws IOException {
    // Read with filter and validate that dictionary encoded values are filtered correctly.
    FilterCompat.Filter filter = FilterCompat.get(pred);
    try (ParquetReader<Group> reader = builder.withFilter(filter).build()) {
      Group group = reader.read();
      int i = 0;
      while (group != null) {
        i += 1;
        group = reader.read();
      }
      assertEquals(expectedCount, i);
    }
  }

  /**
   * Add mock column mappings by {original, encrypted, provider} column path
   * triplets.
   */
  public static void addMappingToMockCellManager(
    CellManager mockCellManager, String[]... originalEncryptedProviders) {
    checkArgument(originalEncryptedProviders.length % 3 == 0,
      "originalEncryptedProviders length must be multiples of 3");

    Set<ColumnPath> originalColumns = new HashSet<>();
    for (int i = 0; i < originalEncryptedProviders.length; i += 3) {
      ColumnPath original = ColumnPath.get(originalEncryptedProviders[i]);
      ColumnPath encrypted = ColumnPath.get(originalEncryptedProviders[i + 1]);
      ColumnPath provider = ColumnPath.get(originalEncryptedProviders[i + 2]);
      when(mockCellManager.getEncryptedColumn(original)).thenReturn(encrypted);
      when(mockCellManager.getProviderColumn(original)).thenReturn(provider);
      when(mockCellManager.getOriginalColumn(encrypted)).thenReturn(original);
      when(mockCellManager.getColumnType(original)).thenReturn(CellManager.CellColumnType.ORIGINAL);
      when(mockCellManager.getColumnType(encrypted)).thenReturn(CellManager.CellColumnType.ENCRYPTED);
      when(mockCellManager.getColumnType(provider)).thenReturn(CellManager.CellColumnType.PROVIDER);
      originalColumns.add(original);
    }
    when(mockCellManager.getOriginalColumns()).thenReturn(originalColumns);
  }

  // Only add original / encrypted column path to cellmanager (exclude provider).
  private static void addOriginalEncryptedToMockCellManager(CellManager mockCellManager,
                                                            String[]... originalEncrypted) {
    checkArgument(originalEncrypted.length % 2 == 0,
      "originalEncryptedProviders length must be multiples of 2");

    Set<ColumnPath> originalColumns = new HashSet<>();
    for (int i = 0; i < originalEncrypted.length; i += 2) {
      ColumnPath original = ColumnPath.get(originalEncrypted[i]);
      ColumnPath encrypted = ColumnPath.get(originalEncrypted[i + 1]);
      when(mockCellManager.getEncryptedColumn(original)).thenReturn(encrypted);
      when(mockCellManager.getOriginalColumn(encrypted)).thenReturn(original);
      when(mockCellManager.getColumnType(original)).thenReturn(CellManager.CellColumnType.ORIGINAL);
      when(mockCellManager.getColumnType(encrypted)).thenReturn(CellManager.CellColumnType.ENCRYPTED);
      originalColumns.add(original);
    }
    when(mockCellManager.getOriginalColumns()).thenReturn(originalColumns);
  }

  private String createTempFile(String file) {
    try {
      return folder.newFile(file).getAbsolutePath();
    } catch (IOException e) {
      throw new AssertionError("Unable to create temporary file", e);
    }
  }

  private Builder createBuilder(String file, MessageType schema,
                                CellManager cellManager, Configuration conf) {
    conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());
    Path path = new Path(file);
    Builder builder = new Builder(path);
    builder.withConf(conf);
    builder.withWriteMode(ParquetFileWriter.Mode.OVERWRITE);
    builder.withCellManager(cellManager);
    return builder;
  }

  public class Builder extends org.apache.parquet.hadoop.ParquetWriter.Builder<Group, Builder> {

    private Builder(Path file) {
      super(file);
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected WriteSupport<Group> getWriteSupport(Configuration conf) {
      return new GroupWriteSupport();
    }
  }
}
