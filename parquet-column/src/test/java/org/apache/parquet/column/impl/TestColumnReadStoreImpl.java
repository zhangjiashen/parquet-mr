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

import java.util.List;
import java.util.Optional;

import org.apache.parquet.column.CellManager;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.mem.MemPageReader;
import org.apache.parquet.column.page.mem.MemPageWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.Version;
import org.apache.parquet.VersionParser;
import org.junit.Test;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestColumnReadStoreImpl {

  private final MessageType SCHEMA = MessageTypeParser.parseMessageType(
    "message test { optional int64 lat; optional binary lat_enc; }");
  private final ColumnDescriptor ENCRYPTED_COLUMN = SCHEMA.getColumns().get(1);
  private final String[] ENCRYPTED_PATH = new String[] { "lat_enc" };
  private final String[] ORIGIN_PATH = new String[] { "lat" };
  private final int ROWS = 131;

  // Expected test converter counter value
  //
  // This test suite writes and reads mixed data (row_index % 4 == 0) or null rows.
  // Every row index % 4 == 0 contains data. Both test LongConverter / BinaryConverter
  // increment expected counter by 4, thus after test run the expected counter will
  // be total row count align up to divisable by 4.
  private final int CONVERTER_COUNTER_END = (ROWS + 3) & ~3;

  @Test
  public void testNoCellManager() {
    // no cell manager, read as binary column
    BinaryConverter converter = new BinaryConverter();
    readBinaryColumn((pageReadStore, recordConverter) -> {
      when(recordConverter.getConverter(1)).thenReturn(converter);
      return new ColumnReadStoreImpl(pageReadStore, recordConverter, SCHEMA, "");
    });
    assertEquals(CONVERTER_COUNTER_END, converter.count);
  }

  @Test
  public void testNullCellManager() {
    // null cell manager, read as binary column
    BinaryConverter converter = new BinaryConverter();
    readBinaryColumn((pageReadStore, recordConverter) -> {
      when(recordConverter.getConverter(1)).thenReturn(converter);
      return new ColumnReadStoreImpl(pageReadStore, recordConverter, SCHEMA, "", null);
    });
    assertEquals(CONVERTER_COUNTER_END, converter.count);
  }

  @Test
  public void testNotEncryptedColumn() {
    // cell manager reports not encrypted column (null path), read as binary column
    BinaryConverter converter = new BinaryConverter();
    readBinaryColumn((pageReadStore, recordConverter) -> {
      when(recordConverter.getConverter(1)).thenReturn(converter);
      CellManager cellManager = mock(CellManager.class);
      return new ColumnReadStoreImpl(pageReadStore, recordConverter, SCHEMA, "", cellManager);
    });
    assertEquals(CONVERTER_COUNTER_END, converter.count);
  }

  @Test
  public void testNotEncryptedColumnEmptyPath() {
    // cell manager reports not encrypted column (empty path), read as binary column
    BinaryConverter converter = new BinaryConverter();
    readBinaryColumn((pageReadStore, recordConverter) -> {
      when(recordConverter.getConverter(1)).thenReturn(converter);
      CellManager cellManager = mock(CellManager.class);
      when(cellManager.getOriginalColumn(ENCRYPTED_PATH)).thenReturn(new String[]{});
      return new ColumnReadStoreImpl(pageReadStore, recordConverter, SCHEMA, "", cellManager);
    });
    assertEquals(CONVERTER_COUNTER_END, converter.count);
  }

  @Test
  public void testEncryptedColumn() {
    // encrypted column, read / decrypt to original column
    LongConverter converter = new LongConverter();
    readBinaryColumn((pageReadStore, recordConverter) -> {
      when(recordConverter.getConverter(0)).thenReturn(converter);

      CellManager cellManager = mock(CellManager.class);
      when(cellManager.getOriginalColumn(ENCRYPTED_PATH)).thenReturn(ORIGIN_PATH);
      when(cellManager.decryptLong(any())).thenAnswer(
        // bar? -> ?
        invocation -> Long.valueOf(
          Binary.fromConstantByteArray((byte[])invocation.getArguments()[0]).toStringUsingUTF8().substring(3)));

      return new ColumnReadStoreImpl(pageReadStore, recordConverter, SCHEMA, "", cellManager);
    });
    assertEquals(CONVERTER_COUNTER_END, converter.count);
  }

  @Test
  public void testNotRequestOriginalColumn() {
    // request only encrypted column, not original column
    MessageType requestSchema = MessageTypeParser.parseMessageType(
      "message test { optional binary lat_enc; }");

    CellManager cellManager = mock(CellManager.class);
    when(cellManager.getOriginalColumn(ENCRYPTED_PATH)).thenReturn(ORIGIN_PATH);

    readBinaryColumn((pageReadStore, recordConverter) -> {
      return new ColumnReadStoreImpl(pageReadStore, recordConverter, requestSchema, "", cellManager);
    });

    verify(cellManager, never()).decryptLong(any());
  }

  /**
   * Prepare test data, get ColumnReadStore/ColumnReader to read the test binary
   * column.
   */
  private void readBinaryColumn(ColumnReadStoreFactory factory) {
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 columnWriterV2 = new ColumnWriterV2(ENCRYPTED_COLUMN, pageWriter,
      ParquetProperties.builder()
        .withDictionaryPageSize(1024).withWriterVersion(PARQUET_2_0)
        .withPageSize(2048).build());
    for (int i = 0; i < ROWS; i++) {
      // write a mix of data and null
      if ((i % 4) == 0) {
        columnWriterV2.write(Binary.fromString("bar" + i), 0, 0);
      } else {
        columnWriterV2.writeNull(0, 0);
      }
      if ((i + 1) % 1000 == 0) {
        columnWriterV2.writePage();
      }
    }
    columnWriterV2.writePage();
    columnWriterV2.finalizeColumnChunk();
    List<DataPage> pages = pageWriter.getPages();
    int valueCount = 0;
    int rowCount = 0;
    for (DataPage dataPage : pages) {
      valueCount += dataPage.getValueCount();
      rowCount += ((DataPageV2)dataPage).getRowCount();
    }
    assertEquals(ROWS, rowCount);
    assertEquals(ROWS, valueCount);
    MemPageReader pageReader = new MemPageReader(ROWS, pages.iterator(), pageWriter.getDictionaryPage());

    PageReadStore pageReadStore = mock(PageReadStore.class);
    when(pageReadStore.getPageReader(ENCRYPTED_COLUMN)).thenReturn(pageReader);
    when(pageReadStore.getRowCount()).thenReturn((long)ROWS);
    when(pageReadStore.getRowIndexes()).thenReturn(Optional.empty());

    GroupConverter recordConverter = mock(GroupConverter.class);
    when(recordConverter.asGroupConverter()).thenReturn(recordConverter);

    // create test ColumnReadStore
    ColumnReadStore columnReadStore = factory.get(pageReadStore, recordConverter);

    // read the test binary column
    ColumnReader columnReader = columnReadStore.getColumnReader(ENCRYPTED_COLUMN);
    for (int i = 0; i < ROWS; i++) {
      assertEquals(0, columnReader.getCurrentRepetitionLevel());
      assertEquals(0, columnReader.getCurrentDefinitionLevel());
      if ((i % 4) == 0) {
        columnReader.writeCurrentValueToConverter();
      }
      columnReader.consume();
    }
  }

  private interface ColumnReadStoreFactory {
    ColumnReadStore get(PageReadStore pageReadStore, GroupConverter recordConverter);
  }

  private static final class LongConverter extends PrimitiveConverter {
    int count;

    @Override
    public void addLong(long value) {
      assertEquals(count, value);
      count += 4;
    }
  }

  private static final class BinaryConverter extends PrimitiveConverter {
    int count;

    @Override
    public void addBinary(Binary value) {
      assertEquals("bar" + count, value.toStringUsingUTF8());
      count += 4;
    }
  }

}
