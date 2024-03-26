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
package org.apache.parquet.hadoop;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.CellManager;
import org.apache.parquet.column.impl.BasicCellManager;
import org.apache.parquet.crypto.CryptoClassLoader;
import org.apache.parquet.crypto.SampleCellManagerFactory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.Test;

public class TestCellEncryptionReader {
  private final static String SCHEMA = "message test { "
    + "required int32 id; "
    + "optional int64 lat; "
    + "optional binary lat_encr; "
    + "} ";

  private final static String REQUEST_ORIGINAL_COLUMN_ONLY = "message test { "
    + "required int32 id; "
    + "optional int64 lat; "
    + "} ";

  private final static String REQUEST_ENCRYPTED_COLUMN_ONLY = "message test { "
    + "required int32 id; "
    + "optional binary lat_encr; "
    + "} ";

  private final static int ROWS = 1000;

  // Use a BasicCellManager for encrypt/decrypt
  private final CellManager basicCellManager = new BasicCellManager();

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    Path file = new Path(tmpDir.newFolder().getAbsolutePath(), "cell.parquet");

    writeTestFile(conf, file);

    // If reader has no CellManager, should read as is
    verifyReadRaw(ParquetReader.builder(new GroupReadSupport(), file)
      .withConf(conf));

    // If reader's CellManager not recognizing column mapping, should read as is
    verifyReadRaw(ParquetReader.builder(new GroupReadSupport(), file)
      .withConf(conf).withCellManager(mock(CellManager.class)));

    // reader has CellManager, should decrypt
    CellManager cellManager = mock(CellManager.class);
    when(cellManager.getOriginalColumn(new String[] { "lat_encr" })).thenReturn(new String[] { "lat" });
    when(cellManager.decryptLong(any())).thenAnswer(
      invocation -> basicCellManager.decryptLong((byte[]) invocation.getArguments()[0]));
    verifyReadDecrypt(ParquetReader.builder(new GroupReadSupport(), file)
      .withConf(conf).withCellManager(cellManager));

    // reader has CellManager from conf, should decrypt
    // (Note that this depends on SampleCellManager extending BasicCellManager)
    conf.set(CryptoClassLoader.CRYPTO_CELL_MANAGER_FACTORY_CLASS, SampleCellManagerFactory.class.getName());
    verifyReadDecrypt(ParquetReader.builder(new GroupReadSupport(), file)
      .withConf(conf));

    // test request schema missing encrypted columns
    conf.set(ReadSupport.PARQUET_READ_SCHEMA, REQUEST_ORIGINAL_COLUMN_ONLY);
    verifyReadDecrypt(ParquetReader.builder(new GroupReadSupport(), file)
      .withConf(conf), true, false);

    // test request schema missing original columns
    conf.set(ReadSupport.PARQUET_READ_SCHEMA, REQUEST_ENCRYPTED_COLUMN_ONLY);
    verifyReadDecrypt(ParquetReader.builder(new GroupReadSupport(), file)
      .withConf(conf), false, true);
  }

  @Test
  public void testKeyNotFoundException() throws Throwable {
    Configuration conf = new Configuration();
    Path file = new Path(tmpDir.newFolder().getAbsolutePath(), "except.parquet");
    writeTestFile(conf, file);

    CellManager cellManager = mock(CellManager.class);
    when(cellManager.getOriginalColumn(new String[] { "lat_encr" })).thenReturn(new String[] { "lat" });
    when(cellManager
      .getEncryptedColumn(ColumnPath.get("lat")))
      .thenReturn(ColumnPath.get("lat_encr"));
    when(cellManager.decryptLong(any())).thenAnswer(
      invocation -> {
        long i = basicCellManager.decryptLong((byte[]) invocation.getArguments()[0]);
        if (shouldNullMask((int) i)) {
          throw new CellManager.KeyNotFoundException("foo key not found");
        }
        return i;
      });

    verifyReadNullMask(
      ParquetReader.builder(new GroupReadSupport(), file)
        .withConf(conf).withCellManager(cellManager),
      true, true);

    // test read original column only
    conf.set(ReadSupport.PARQUET_READ_SCHEMA, REQUEST_ORIGINAL_COLUMN_ONLY);
    verifyReadNullMask(
      ParquetReader.builder(new GroupReadSupport(), file)
        .withConf(conf).withCellManager(cellManager),
      true, false);

    // test read encrypted column only
    conf.set(ReadSupport.PARQUET_READ_SCHEMA, REQUEST_ENCRYPTED_COLUMN_ONLY);
    verifyReadNullMask(
      ParquetReader.builder(new GroupReadSupport(), file)
        .withConf(conf).withCellManager(cellManager),
      false, true);
  }

  @Test
  public void testCryptoException() throws Throwable {
    Configuration conf = new Configuration();
    Path file = new Path(tmpDir.newFolder().getAbsolutePath(), "except.parquet");
    writeTestFile(conf, file);

    CellManager cellManager = mock(CellManager.class);
    when(cellManager.getOriginalColumn(new String[] { "lat_encr" })).thenReturn(new String[] { "lat" });
    when(cellManager.decryptLong(any())).thenThrow(
      new CellManager.CryptoException("unexpected encryption format"));
    try {
      verifyReadDecrypt(ParquetReader.builder(new GroupReadSupport(), file)
        .withConf(conf).withCellManager(cellManager));
    } catch (ParquetDecodingException e) {
      assertTrue(
        e.getMessage(),
        e.getMessage().contains("CryptoException: unexpected encryption format"));
    }
  }

  @Test
  public void testAccessDeniedException() throws Throwable {
    Configuration conf = new Configuration();
    Path file = new Path(tmpDir.newFolder().getAbsolutePath(), "except.parquet");
    writeTestFile(conf, file);

    CellManager cellManager = mock(CellManager.class);
    when(cellManager.getOriginalColumn(new String[] { "lat_encr" })).thenReturn(new String[] { "lat" });
    when(cellManager.decryptLong(any())).thenThrow(
      new CellManager.AccessDeniedException("access denied to key"));
    try {
      verifyReadDecrypt(ParquetReader.builder(new GroupReadSupport(), file)
        .withConf(conf).withCellManager(cellManager));
    } catch (ParquetDecodingException e) {
      assertTrue(
        e.getMessage(),
        e.getMessage().contains("AccessDeniedException: access denied to key"));
    }
  }

  private void writeTestFile(Configuration conf, Path file) throws Exception {
    MessageType schema = parseMessageType(SCHEMA);
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

    // create a mix of test rows: full nulls, data in encrypted column, data in
    // original column
    try (ParquetWriter<Group> writer = new GroupWriterBuilder(file).withConf(conf).build()) {
      for (int i = 0; i < ROWS; i++) {
        Group group = groupFactory.newGroup().append("id", i);
        if (!shouldNullRow(i)) {
          if (shouldEncryptRow(i)) {
            group.add("lat_encr", Binary.fromConstantByteArray(
              basicCellManager.encryptLong(i, BasicCellManager.BASIC_PROVIDER_VAL)));
          } else {
            group.add("lat", (long) i);
          }
        }
        writer.write(group);
      }
    }
  }

  // Verify when the reader should just read as is
  private void verifyReadRaw(ParquetReader.Builder<Group> builder) throws Exception {
    try (ParquetReader<Group> reader = builder.build()) {
      for (int i = 0; i < ROWS; i++) {
        Group group = reader.read();
        assertEquals(i, group.getInteger("id", 0));
        if (shouldNullRow(i)) {
          // origin: null, encrypted: null
          assertEquals(0, group.getFieldRepetitionCount("lat")); // null
          assertEquals(0, group.getFieldRepetitionCount("lat_encr")); // null
        } else if (shouldEncryptRow(i)) {
          // origin: null, encrypted: value
          assertEquals(0, group.getFieldRepetitionCount("lat")); // null
          assertEquals(
            Binary.fromConstantByteArray(
              basicCellManager.encryptLong(i, BasicCellManager.BASIC_PROVIDER_VAL)),
            group.getBinary("lat_encr", 0));
        } else {
          // origin: value, encrypted: null
          assertEquals(i, group.getLong("lat", 0));
          assertEquals(0, group.getFieldRepetitionCount("lat_encr")); // null
        }
      }
    }
  }

  // Verify when the reader should decrypt
  private void verifyReadDecrypt(ParquetReader.Builder<Group> builder) throws Exception {
    verifyReadDecrypt(builder, true, true);
  }

  private void verifyReadDecrypt(ParquetReader.Builder<Group> builder,
                                 boolean requestedOriginColumn, boolean requestedEncryptedColumn) throws Exception {
    try (ParquetReader<Group> reader = builder.build()) {
      for (int i = 0; i < ROWS; i++) {
        Group group = reader.read();
        assertEquals(i, group.getInteger("id", 0));
        if (shouldNullRow(i)) {
          // origin: null, encrypted: null
          if (requestedOriginColumn) {
            assertEquals(0, group.getFieldRepetitionCount("lat")); // null
          }
          if (requestedEncryptedColumn) {
            assertEquals(0, group.getFieldRepetitionCount("lat_encr")); // null
          }
        } else {
          // origin: value, encrypted: null
          if (requestedOriginColumn) {
            assertEquals(i, group.getLong("lat", 0));
          }
          if (requestedEncryptedColumn) {
            assertEquals(0, group.getFieldRepetitionCount("lat_encr")); // null
          }
        }
      }
    }
  }

  // Verify when KeyDeleted reader should return NULLs from encrypted column
  private void verifyReadNullMask(
    ParquetReader.Builder<Group> builder,
    boolean requestedOriginColumn, boolean requestedEncryptedColumn) throws Exception {
    try (ParquetReader<Group> reader = builder.build()) {
      for (int i = 0; i < ROWS; i++) {
        Group group = reader.read();
        assertEquals(i, group.getInteger("id", 0));
        if (requestedOriginColumn) {
          if (shouldNullRow(i) || shouldNullMask(i)) {
            // origin: null or null-mask
            assertEquals(0, group.getFieldRepetitionCount("lat")); // null
          } else {
            // origin: value
            assertEquals(i, group.getLong("lat", 0));
          }
        }
        if (requestedEncryptedColumn) {
          // encrypted: always null
          assertEquals(0, group.getFieldRepetitionCount("lat_encr")); // null
        }
      }
    }

  }

  // have some rows contain full nulls
  private boolean shouldNullRow(int rowIndex) {
    return (rowIndex & 6) == 0;
  }

  // have some in encrypted column
  private boolean shouldEncryptRow(int rowIndex) {
    return (rowIndex & 4) != 0;
  }

  // have a subset encrypted column null mask
  private boolean shouldNullMask(int rowIndex) {
    return (rowIndex & 6) == 6;
  }

  static class GroupWriterBuilder extends ParquetWriter.Builder<Group, GroupWriterBuilder> {
    public GroupWriterBuilder(Path path) {
      super(path);
    }

    @Override
    protected GroupWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<Group> getWriteSupport(Configuration conf) {
      return new GroupWriteSupport();
    }
  }

}
