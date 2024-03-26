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

import java.util.Optional;
import java.util.PrimitiveIterator;

import org.apache.parquet.VersionParser;
import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.VersionParser.VersionParseException;
import org.apache.parquet.column.CellManager;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * Implementation of the ColumnReadStore
 *
 * Initializes individual columns based on schema and converter
 */
public class ColumnReadStoreImpl implements ColumnReadStore {

  private final PageReadStore pageReadStore;
  private final GroupConverter recordConverter;
  private final MessageType schema;
  private final ParsedVersion writerVersion;
  private final CellManager cellManager; // optional

  /**
   * @param pageReadStore underlying page storage
   * @param recordConverter the user provided converter to materialize records
   * @param schema the schema we are reading
   * @param createdBy writer version string from the Parquet file being read
   */
  public ColumnReadStoreImpl(PageReadStore pageReadStore,
                             GroupConverter recordConverter,
                             MessageType schema, String createdBy) {
    this(pageReadStore, recordConverter, schema, createdBy, /*cellManager*/null);
  }

  public ColumnReadStoreImpl(PageReadStore pageReadStore,
                             GroupConverter recordConverter,
                             MessageType schema, String createdBy,
                             CellManager cellManager) {
    super();
    this.pageReadStore = pageReadStore;
    this.recordConverter = recordConverter;
    this.schema = schema;
    this.cellManager = cellManager;

    ParsedVersion version;
    try {
      version = VersionParser.parse(createdBy);
    } catch (RuntimeException | VersionParseException e) {
      version = null;
    }
    this.writerVersion = version;
  }

  @Override
  public ColumnReader getColumnReader(ColumnDescriptor path) {
    PageReader pageReader = pageReadStore.getPageReader(path);
    if (pageReader.isNullMaskedColumn()) {
      return null;
    }

    // try retrieve original column path (if this column is the encrypted column)
    String[] originPath = cellManager != null ? cellManager.getOriginalColumn(path.getPath()) : null;

    PrimitiveConverter converter;
    if (originPath != null && originPath.length > 0) {
      // The given encrypted column has a matching original column. Create a
      // wrapper converter to decrypt and pipe to the original column value
      // converter.
      converter = getPrimitiveConverter(originPath, true);
    } else {
      // non-encrypted column, retrieve its own primitive converter as is
      converter = getPrimitiveConverter(path.getPath(), false);
    }

    Optional<PrimitiveIterator.OfLong> rowIndexes = pageReadStore.getRowIndexes();
    if (rowIndexes.isPresent()) {
      return new SynchronizingColumnReader(path, pageReader, converter, writerVersion, rowIndexes.get());
    } else {
      return new ColumnReaderImpl(path, pageReader, converter, writerVersion);
    }
  }

  private PrimitiveConverter getPrimitiveConverter(String[] path, boolean cellConvert) {
    Type currentType = schema;
    Converter currentConverter = recordConverter;
    for (String fieldName : path) {
      final GroupType groupType = currentType.asGroupType();
      if (cellConvert && !groupType.containsField(fieldName)) {
        // original column not requested, return a dummy converter to discard values
        return new CellPrimitiveConverter();
      }
      int fieldIndex = groupType.getFieldIndex(fieldName);
      currentType = groupType.getType(fieldName);
      currentConverter = currentConverter.asGroupConverter().getConverter(fieldIndex);
    }
    PrimitiveConverter converter = currentConverter.asPrimitiveConverter();
    if (cellConvert) {
      // request a wrapper converter to receive from encrypted column value
      converter = new CellPrimitiveConverter(cellManager, currentType.asPrimitiveType(), converter);
    }
    return converter;
  }

}
