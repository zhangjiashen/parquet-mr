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
package org.apache.parquet.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.parquet.column.*;
import org.apache.parquet.column.impl.*;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.compat.FilterCompat.FilterPredicateCompat;
import org.apache.parquet.filter2.compat.FilterCompat.NoOpFilter;
import org.apache.parquet.filter2.compat.FilterCompat.UnboundRecordFilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Visitor;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.recordlevel.FilteringRecordMaterializer;
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate;
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicateBuilder;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Message level of the IO structure
 */
public class MessageColumnIO extends GroupColumnIO {
  private static final Logger LOG = LoggerFactory.getLogger(MessageColumnIO.class);

  private static final boolean DEBUG = LOG.isDebugEnabled();

  private List<PrimitiveColumnIO> leaves;

  private final boolean validating;
  private final String createdBy;
  private CellManager cellManager; // optional

  MessageColumnIO(MessageType messageType, boolean validating, String createdBy, CellManager cellManager) {
    super(messageType, null, 0);
    this.validating = validating;
    this.createdBy = createdBy;
    this.cellManager = cellManager;
  }

  @Override
  public List<String[]> getColumnNames() {
    return super.getColumnNames();
  }

  public <T> RecordReader<T> getRecordReader(PageReadStore columns,
                                             RecordMaterializer<T> recordMaterializer) {
    return getRecordReader(columns, recordMaterializer, FilterCompat.NOOP);
  }

  /**
   * @param columns a page read store with the column data
   * @param recordMaterializer a record materializer
   * @param filter a record filter
   * @param <T> the type of records returned by the reader
   * @return a record reader
   * @deprecated use getRecordReader(PageReadStore, RecordMaterializer, Filter)
   */
  @Deprecated
  public <T> RecordReader<T> getRecordReader(PageReadStore columns,
                                             RecordMaterializer<T> recordMaterializer,
                                             UnboundRecordFilter filter) {
    return getRecordReader(columns, recordMaterializer, FilterCompat.get(filter));
  }

  public <T> RecordReader<T> getRecordReader(final PageReadStore columns,
                                             final RecordMaterializer<T> recordMaterializer,
                                             final Filter filter) {
    Objects.requireNonNull(columns, "columns cannot be null");
    Objects.requireNonNull(recordMaterializer, "recordMaterializer cannot be null");
    Objects.requireNonNull(filter, "filter cannot be null");

    if (leaves.isEmpty()) {
      return new EmptyRecordReader<>(recordMaterializer);
    }

    return filter.accept(new Visitor<RecordReader<T>>() {
      @Override
      public RecordReader<T> visit(FilterPredicateCompat filterPredicateCompat) {

        FilterPredicate predicate = filterPredicateCompat.getFilterPredicate();
        IncrementallyUpdatedFilterPredicateBuilder builder = new IncrementallyUpdatedFilterPredicateBuilder(leaves);
        IncrementallyUpdatedFilterPredicate streamingPredicate = builder.build(predicate);
        RecordMaterializer<T> filteringRecordMaterializer = new FilteringRecordMaterializer<T>(
            recordMaterializer,
            leaves,
            builder.getValueInspectorsByColumn(),
            streamingPredicate);

        return new RecordReaderImplementation<>(
            MessageColumnIO.this,
            filteringRecordMaterializer,
            validating,
            new ColumnReadStoreImpl(columns, filteringRecordMaterializer.getRootConverter(), getType(), createdBy, cellManager));
      }

      @Override
      public RecordReader<T> visit(UnboundRecordFilterCompat unboundRecordFilterCompat) {
        return new FilteredRecordReader<>(
            MessageColumnIO.this,
            recordMaterializer,
            validating,
            new ColumnReadStoreImpl(columns, recordMaterializer.getRootConverter(), getType(), createdBy, cellManager),
            unboundRecordFilterCompat.getUnboundRecordFilter(),
            columns.getRowCount()
        );
      }

      @Override
      public RecordReader<T> visit(NoOpFilter noOpFilter) {
        return new RecordReaderImplementation<>(
            MessageColumnIO.this,
            recordMaterializer,
            validating,
            new ColumnReadStoreImpl(columns, recordMaterializer.getRootConverter(), getType(), createdBy, cellManager));
      }
    });
  }

  /**
   * To improve null writing performance, we cache null values on group nodes. We flush nulls when a
   * non-null value hits the group node.
   *
   * Intuitively, when a group node hits a null value, all the leaves underneath it should be null.
   * A direct way of doing it is to write nulls for all the leaves underneath it when a group node
   * is null. This approach is not optimal, consider following case:
   *
   *    - When the schema is really wide where for each group node, there are thousands of leaf
   *    nodes underneath it.
   *    - When the data being written is really sparse, group nodes could hit nulls frequently.
   *
   * With the direct approach, if a group node hit null values a thousand times, and there are a
   * thousand nodes underneath it.
   * For each null value, it iterates over a thousand leaf writers to write null values and it
   *  will do it for a thousand null values.
   *
   * In the above case, each leaf writer maintains it's own buffer of values, calling thousands of
   * them in turn is very bad for memory locality. Instead each group node can remember the null values
   * encountered and flush only when a non-null value hits the group node. In this way, when we flush
   * null values, we only iterate through all the leaves 1 time and multiple cached null values are
   * flushed to each leaf in a tight loop. This implementation has following characteristics.
   *
   *    1. When a group node hits a null value, it adds the repetition level of the null value to
   *    the groupNullCache. The definition level of the cached nulls should always be the same as
   *    the definition level of the group node so there is no need to store it.
   *
   *    2. When a group node hits a non null value and it has null value cached, it should flush null
   *    values and start from his children group nodes first. This make sure the order of null values
   *     being flushed is correct.
   *
   */
  private class MessageColumnIORecordConsumer extends RecordConsumer {
    private ColumnIO currentColumnIO;
    private int currentLevel = 0;

    private class FieldsMarker {
      private BitSet visitedIndexes = new BitSet();

      @Override
      public String toString() {
        return "VisitedIndex{" +
            "visitedIndexes=" + visitedIndexes +
            '}';
      }

      public void reset(int fieldsCount) {
        this.visitedIndexes.clear(0, fieldsCount);
      }

      public void markWritten(int i) {
        visitedIndexes.set(i);
      }

      public boolean isWritten(int i) {
        return visitedIndexes.get(i);
      }
    }

    //track at each level of depth, which fields are written, so nulls can be inserted for the unwritten fields
    private final FieldsMarker[] fieldsWritten;
    private final int[] r;
    private final ColumnWriter[] columnWriters;

    /**
     * Maintain a map of groups and all the leaf nodes underneath it. It's used to optimize writing null for a group node.
     * Instead of using recursion calls, all the leaves can be called directly without traversing the sub tree of the group node
     */
    private Map<GroupColumnIO, List<ColumnWriter>> groupToLeafWriter = new HashMap<>();

    /**
     * Maintain a map from Original Columns to the Index of their sibling encrypted leaf.
     *
     */
    private Map<String[], Integer> originalColumnToEncryptedLeafIndex = new HashMap<>();

    /*
     * Cache nulls for each group node. It only stores the repetition level, since the definition level
     * should always be the definition level of the group node.
     */
    private Map<GroupColumnIO, IntArrayList> groupNullCache = new HashMap<>();
    // Cache all bufferedColumnWriters because we need a reference to them to call flush at endMessage time.
    private final Map<ColumnDescriptor, BufferedColumnWriter> originalColumnToBufferedWriter = new HashMap<>();
    // Cache a mapping between original column and its notify column id (if any).
    // In pass through mode - the encrypted column is wrapped as a NotifyColumn
    // In regular cell encryption - the "provider" column is wrapped as a NotifyColumn.
    private Map<ColumnDescriptor, Integer> originalColToNotifyColId = new HashMap<>();
    private final ColumnWriteStore columns;
    private boolean emptyField = true;
    private final CellManager cellManager;

    private void buildGroupToLeafWriterMap(PrimitiveColumnIO primitive, ColumnWriter writer) {
      GroupColumnIO parent = primitive.getParent();
      do {
        getLeafWriters(parent).add(writer);
        parent = parent.getParent();
      } while (parent != null);
    }

    private List<ColumnWriter> getLeafWriters(GroupColumnIO group) {
      List<ColumnWriter> writers = groupToLeafWriter.get(group);
      if (writers == null) {
        writers = new ArrayList<>();
        groupToLeafWriter.put(group, writers);
      }
      return writers;
    }

    private void addNotifyColumnObservers() {
      if (cellManager == null) {
        // Nothing to observe because rootcomply is disabled.
        return;
      }
      for (Map.Entry<ColumnDescriptor, BufferedColumnWriter> buffered : originalColumnToBufferedWriter.entrySet()) {
        ColumnDescriptor originalDesc = buffered.getKey();
        BufferedColumnWriter bufferedColumnWriter = buffered.getValue();
        NotifyColumnWriter notifyWriter =
          (NotifyColumnWriter) columnWriters[originalColToNotifyColId.get(originalDesc)];
        notifyWriter.addObserver(bufferedColumnWriter);
      }
    }

    private PrimitiveColumnIO getEncryptedColIO(PrimitiveColumnIO originalColumnIO) {
      String[] encryptedColumn = cellManager.getEncryptedColumn(originalColumnIO.getFieldPath());
      if (encryptedColumn == null || encryptedColumn.length == 0) {
        throw new IllegalStateException("Configured encrypted column path is empty or invalid" +
          " for the original path = " + Arrays.toString(originalColumnIO.getFieldPath()));
      }
      return getColumnIOWithPath(originalColumnIO, encryptedColumn);
    }

    private PrimitiveColumnIO getProviderColIO(PrimitiveColumnIO originalColumnIO) {
      String[] providerPath = cellManager.getProviderColumn(originalColumnIO.getFieldPath());
      if (providerPath == null || providerPath.length == 0) {
        throw new IllegalStateException("Configured provider column path is empty or invalid" +
          " for the original path = " + Arrays.toString(originalColumnIO.getFieldPath()));
      }
      return getColumnIOWithPath(originalColumnIO, providerPath);
    }

    // Columns marked as original by CellManager are guaranteed to have a sibling Encrypt column.
    private ColumnWriter getEncryptedColumnWriter(ColumnWriteStore columns,
                                                  PrimitiveColumnIO originalColumnIO) {
      String[] encryptedColPath = cellManager.getEncryptedColumn(originalColumnIO.getFieldPath());
      if (encryptedColPath == null || encryptedColPath.length == 0) {
        throw new IllegalStateException("Configured encrypted column path is empty or invalid " +
          "for the original path = " + Arrays.toString(originalColumnIO.getFieldPath()));
      }
      String encryptedLeaf = encryptedColPath[encryptedColPath.length - 1];
      PrimitiveColumnIO encryptedColumnIO = (PrimitiveColumnIO) originalColumnIO.getParent().getChild(encryptedLeaf);
      if (encryptedColumnIO.getPrimitive() != PrimitiveType.PrimitiveTypeName.BINARY) {
        throw new UnsupportedOperationException("Non-binary encrypted columns not supported by cell encryption");
      }
      originalColumnToEncryptedLeafIndex.put(originalColumnIO.getFieldPath(), encryptedColumnIO.getIndex());
      return columns.getColumnWriter(encryptedColumnIO.getColumnDescriptor());
    }

    // Decorate ORIGINAL / ENCRYPTED columns to write pre cell encrypted data while maintaining correct stats
    private ColumnWriter decorateForPassThrough(ColumnWriter columnWriter,
                                                PrimitiveColumnIO primitiveColumnIO) {
      CellManager.CellColumnType cellColumnType = cellManager.getColumnType(ColumnPath
        .get(primitiveColumnIO.getFieldPath()));
      if (cellColumnType == CellManager.CellColumnType.ORIGINAL) {
        PrimitiveColumnIO encryptedColIO = getEncryptedColIO(primitiveColumnIO);
        StatsBufferedColumnWriter statsColumnWriter = new StatsBufferedColumnWriter(columnWriter,
          cellManager, primitiveColumnIO.getPrimitive(),
          primitiveColumnIO.getName(), encryptedColIO.getName());
        originalColumnToBufferedWriter
          .put(primitiveColumnIO.getColumnDescriptor(), statsColumnWriter);
        originalColToNotifyColId.put(primitiveColumnIO.getColumnDescriptor(),
          encryptedColIO.getId());
        return statsColumnWriter;
      } else if (cellColumnType == CellManager.CellColumnType.ENCRYPTED) {
        return new NotifyColumnWriter(columnWriter);
      }
      return columnWriter;
    }

    // Decorate the ORIGINAL/ENCRYPTED/PROVIDER ColumnWriters for cell encryption if needed
    private ColumnWriter decorateForCellEncryption(ColumnWriter columnWriter, PrimitiveColumnIO primitiveColumnIO) {
      CellManager.CellColumnType cellColumnType = cellManager.getColumnType(ColumnPath
        .get(primitiveColumnIO.getFieldPath()));
      if (cellColumnType == CellManager.CellColumnType.ORIGINAL) {
        PrimitiveColumnIO providerColIO = getProviderColIO(primitiveColumnIO);
        ColumnWriter encryptedColumnWriter = getEncryptedColumnWriter(columns, primitiveColumnIO);
        int definitionLevelToWriteNull = primitiveColumnIO.getParent().getDefinitionLevel();
        int maxProviderRepetitionLevel = providerColIO.getRepetitionLevel();
        DualBufferedColumnWriter bcw = new DualBufferedColumnWriter(columnWriter, encryptedColumnWriter,
          cellManager, primitiveColumnIO.getPrimitive(),
          definitionLevelToWriteNull, maxProviderRepetitionLevel);
        // Cache the buffered writers to flush them at endMessage time efficiently.
        originalColumnToBufferedWriter.put(primitiveColumnIO.getColumnDescriptor(), bcw);
        originalColToNotifyColId.put(primitiveColumnIO.getColumnDescriptor(),
          providerColIO.getId());
        if (DEBUG) LOG.debug("Initialized RootComply DualBufferedColumnWriter" +
          " for col = {}", Arrays.toString(primitiveColumnIO.getFieldPath()));
        return bcw;
      } else if (cellColumnType == CellManager.CellColumnType.ENCRYPTED) {
        if (DEBUG) LOG.debug("Initialized RootComply DiscardColumnWriter" +
          " for col = {}", Arrays.toString(primitiveColumnIO.getFieldPath()));
        return new DiscardColumnWriter(columnWriter);
      } else if (cellColumnType == CellManager.CellColumnType.PROVIDER) {
        if (DEBUG) LOG.debug("Initialized RootComply NotifyColumnWriter" +
          " for col = {}", Arrays.toString(primitiveColumnIO.getFieldPath()));
        return new NotifyColumnWriter(columnWriter);
      }
      return columnWriter;
    }

    private ColumnWriter getColumnWriterFromWriteStore(ColumnWriteStore columns,
                                                       PrimitiveColumnIO primitiveColumnIO) {
      ColumnWriter columnWriter = columns.getColumnWriter(primitiveColumnIO.getColumnDescriptor());
      if (cellManager == null) {
        return columnWriter;
      } else if (cellManager.isPassThroughModeEnabled()) {
        return decorateForPassThrough(columnWriter, primitiveColumnIO);
      } else {
        return decorateForCellEncryption(columnWriter, primitiveColumnIO);
      }
    }

    public MessageColumnIORecordConsumer(ColumnWriteStore columns, CellManager cellManager) {
      this.columns = columns;
      int maxDepth = 0;
      this.columnWriters = new ColumnWriter[MessageColumnIO.this.getLeaves().size()];
      this.cellManager = cellManager;

      for (PrimitiveColumnIO primitiveColumnIO : MessageColumnIO.this.getLeaves()) {
        ColumnWriter w = getColumnWriterFromWriteStore(columns, primitiveColumnIO);
        maxDepth = Math.max(maxDepth, primitiveColumnIO.getFieldPath().length);
        columnWriters[primitiveColumnIO.getId()] = w;
        buildGroupToLeafWriterMap(primitiveColumnIO, w);
      }

      addNotifyColumnObservers();
      fieldsWritten = new FieldsMarker[maxDepth];
      for (int i = 0; i < maxDepth; i++) {
        fieldsWritten[i] = new FieldsMarker();
      }
      r = new int[maxDepth];
    }

    private void printState() {
      if (DEBUG) {
        log(currentLevel + ", " + fieldsWritten[currentLevel] + ": " + Arrays.toString(currentColumnIO.getFieldPath()) + " r:" + r[currentLevel]);
        if (r[currentLevel] > currentColumnIO.getRepetitionLevel()) {
          // sanity check
          throw new InvalidRecordException(r[currentLevel] + "(r) > " + currentColumnIO.getRepetitionLevel() + " ( schema r)");
        }
      }
    }

    private void log(Object message, Object...parameters) {
      if (DEBUG) {
        StringBuilder indent = new StringBuilder(currentLevel * 2);
        for (int i = 0; i < currentLevel; ++i) {
          indent.append("  ");
        }
        LOG.debug(indent.toString() + message, parameters);
      }
    }

    @Override
    public void startMessage() {
      if (DEBUG) log("< MESSAGE START >");
      currentColumnIO = MessageColumnIO.this;
      r[0] = 0;
      int numberOfFieldsToVisit = ((GroupColumnIO) currentColumnIO).getChildrenCount();
      fieldsWritten[0].reset(numberOfFieldsToVisit);
      if (DEBUG) printState();
    }

    @Override
    public void endMessage() {
      flushBufferedColumnWriters(false);
      writeNullForMissingFieldsAtCurrentLevel();

      // We need to flush the cached null values before ending the record to ensure that everything is sent to the
      // writer before the current page would be closed
      if (columns.isColumnFlushNeeded()) {
        flush();
      }

      columns.endRecord();
      if (DEBUG) log("< MESSAGE END >");
      if (DEBUG) printState();
    }

    @Override
    public void startField(String field, int index) {
      try {
        if (DEBUG) log("startField({}, {})", field, index);
        currentColumnIO = ((GroupColumnIO) currentColumnIO).getChild(index);
        emptyField = true;
        if (DEBUG) printState();
      } catch (RuntimeException e) {
        throw new ParquetEncodingException("error starting field " + field + " at " + index, e);
      }
    }

    @Override
    public void endField(String field, int index) {
      if (DEBUG) log("endField({}, {})",field ,index);
      setFieldsWrittenForCellEncryption(index);
      currentColumnIO = currentColumnIO.getParent();
      if (emptyField) {
        throw new ParquetEncodingException("empty fields are illegal, the field should be ommited completely instead");
      }
      fieldsWritten[currentLevel].markWritten(index);
      r[currentLevel] = currentLevel == 0 ? 0 : r[currentLevel - 1];
      if (DEBUG) printState();
    }

    // Given any ColumnIO (any node in the schema tree) and a path to a leaf column
    // this method traverse to the root and get the ColumnIO specified by the leaf path.
    private PrimitiveColumnIO getColumnIOWithPath(ColumnIO anyColumnIO, String[] leafPath) {
      ColumnIO finalColumnIO = anyColumnIO;
      // Traverse to root columnIO
      while (finalColumnIO.getParent() != null) {
        finalColumnIO = finalColumnIO.getParent();
      }

      // Traverse down to the desired leaf node and return it
      for (String child : leafPath) {
        finalColumnIO = ((GroupColumnIO) finalColumnIO).getChild(child);
      }
      return (PrimitiveColumnIO) finalColumnIO;
    }

    private void flushBufferedColumnWriters(boolean isFinalFlush) {
      if (cellManager == null) {
        // Nothing to flush because RootComply is disabled
        return;
      }
      for (BufferedColumnWriter bufferedColumnWriter : originalColumnToBufferedWriter.values()) {
        bufferedColumnWriter.writeBufferedValues(isFinalFlush);
      }
    }

    private void writeNullForMissingFieldsAtCurrentLevel() {
      int currentFieldsCount = ((GroupColumnIO) currentColumnIO).getChildrenCount();
      for (int i = 0; i < currentFieldsCount; i++) {
        if (!fieldsWritten[currentLevel].isWritten(i)) {
          try {
            ColumnIO undefinedField = ((GroupColumnIO) currentColumnIO).getChild(i);
            int d = currentColumnIO.getDefinitionLevel();
            if (DEBUG) log(Arrays.toString(undefinedField.getFieldPath()) + ".writeNull(" + r[currentLevel] + "," + d + ")");
            writeNull(undefinedField, r[currentLevel], d);
          } catch (RuntimeException e) {
            throw new ParquetEncodingException("error while writing nulls for fields of indexes " + i + " . current index: " + fieldsWritten[currentLevel], e);
          }
        }
      }
    }

    private void writeNull(ColumnIO undefinedField, int r, int d) {
      if (undefinedField.getType().isPrimitive()) {
        columnWriters[((PrimitiveColumnIO) undefinedField).getId()].writeNull(r, d);
      } else {
        GroupColumnIO groupColumnIO = (GroupColumnIO) undefinedField;
        // only cache the repetition level, the definition level should always be the definition level of the parent node
        cacheNullForGroup(groupColumnIO, r);
      }
    }

    private void cacheNullForGroup(GroupColumnIO group, int r) {
      IntArrayList nulls = groupNullCache.get(group);
      if (nulls == null) {
        nulls = new IntArrayList();
        groupNullCache.put(group, nulls);
      }
      nulls.add(r);
    }

    private void writeNullToLeaves(GroupColumnIO group) {
      IntArrayList nullCache = groupNullCache.get(group);
      if (nullCache == null || nullCache.isEmpty())
        return;

      int parentDefinitionLevel = group.getParent().getDefinitionLevel();
      for (ColumnWriter leafWriter : groupToLeafWriter.get(group)) {
        for (IntIterator iter = nullCache.iterator(); iter.hasNext();) {
          int repetitionLevel = iter.nextInt();
          leafWriter.writeNull(repetitionLevel, parentDefinitionLevel);
        }
      }
      nullCache.clear();
    }

    private void setRepetitionLevel() {
      r[currentLevel] = currentColumnIO.getRepetitionLevel();
      if (DEBUG) log("r: {}", r[currentLevel]);
    }

    @Override
    public void startGroup() {
      if (DEBUG) log("startGroup()");
      GroupColumnIO group = (GroupColumnIO) currentColumnIO;

      // current group is not null, need to flush all the nulls that were cached before
      if (hasNullCache(group)) {
        flushCachedNulls(group);
      }

      ++currentLevel;
      r[currentLevel] = r[currentLevel - 1];

      int fieldsCount = ((GroupColumnIO) currentColumnIO).getChildrenCount();
      fieldsWritten[currentLevel].reset(fieldsCount);
      if (DEBUG) printState();
    }

    private boolean hasNullCache(GroupColumnIO group) {
      IntArrayList nulls = groupNullCache.get(group);
      return nulls != null && !nulls.isEmpty();
    }


    private void flushCachedNulls(GroupColumnIO group) {
      //flush children first
      for (int i = 0; i < group.getChildrenCount(); i++) {
        ColumnIO child = group.getChild(i);
        if (child instanceof GroupColumnIO) {
          flushCachedNulls((GroupColumnIO) child);
        }
      }
      //then flush itself
      writeNullToLeaves(group);
    }

    @Override
    public void endGroup() {
      if (DEBUG) log("endGroup()");
      emptyField = false;
      writeNullForMissingFieldsAtCurrentLevel();
      --currentLevel;

      setRepetitionLevel();
      if (DEBUG) printState();
    }

    private ColumnWriter getColumnWriter() {
      return columnWriters[((PrimitiveColumnIO) currentColumnIO).getId()];
    }

    @Override
    public void addInteger(int value) {
      if (DEBUG) log("addInt({})", value);
      emptyField = false;
      getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

      setRepetitionLevel();
      if (DEBUG) printState();
    }

    @Override
    public void addLong(long value) {
      if (DEBUG) log("addLong({})", value);
      emptyField = false;
      getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

      setRepetitionLevel();
      if (DEBUG) printState();
    }

    @Override
    public void addBoolean(boolean value) {
      if (DEBUG) log("addBoolean({})", value);
      emptyField = false;
      getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

      setRepetitionLevel();
      if (DEBUG) printState();
    }

    @Override
    public void addBinary(Binary value) {
      if (DEBUG) log("addBinary({} bytes)", value.length());
      emptyField = false;
      getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

      setRepetitionLevel();
      if (DEBUG) printState();
    }

    @Override
    public void addFloat(float value) {
      if (DEBUG) log("addFloat({})", value);
      emptyField = false;
      getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

      setRepetitionLevel();
      if (DEBUG) printState();
    }

    @Override
    public void addDouble(double value) {
      if (DEBUG) log("addDouble({})", value);
      emptyField = false;
      getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

      setRepetitionLevel();
      if (DEBUG) printState();
    }


    /**
     * Flush null for all groups
     */
    @Override
    public void flush() {
      flushCachedNulls(MessageColumnIO.this);
      // flushCachedNulls will push any remaining nulls to bufferedColumnWriters,
      // need to flush bufferedColumnWriters one last time before closing writer.
      // flushBufferedColumnWriters must be called after flushCachedNulls
      flushBufferedColumnWriters(true);
    }

    private void setFieldsWrittenForCellEncryption(int index) {
      if (cellManager == null || cellManager.isPassThroughModeEnabled()) {
        // Mark all fields as normal when cellManager is disabled or if passThrough mode
        fieldsWritten[currentLevel].markWritten(index);
        return;
      }

      CellManager.CellColumnType currentType = cellManager.getColumnType(ColumnPath.get(currentColumnIO.getFieldPath()));
      if (currentType == CellManager.CellColumnType.ORIGINAL) {
        // Need to mark both original + encrypted as written so parquet doesn't automatically insert nulls.
        fieldsWritten[currentLevel].markWritten(index);
        int encryptIndex = originalColumnToEncryptedLeafIndex.get(currentColumnIO.getFieldPath());
        fieldsWritten[currentLevel].markWritten(encryptIndex);
      } else if (currentType == CellManager.CellColumnType.ENCRYPTED) {
        // Do nothing - let ENCRYPTED fieldsWritten be managed by original column case
      } else {
        // All other column types include PROVIDER or non-cell encryption related columns are markWritten normally.
        fieldsWritten[currentLevel].markWritten(index);
      }
    }
  }

  public RecordConsumer getRecordWriter(ColumnWriteStore columns) {
    return getRecordWriter(columns, null);
  }

  public RecordConsumer getRecordWriter(ColumnWriteStore columns, CellManager cellManager) {
    RecordConsumer recordWriter = new MessageColumnIORecordConsumer(columns, cellManager);
    if (DEBUG) recordWriter = new RecordConsumerLoggingWrapper(recordWriter);
    return validating ? new ValidatingRecordConsumer(recordWriter, getType()) : recordWriter;
  }

  void setLevels() {
    setLevels(0, 0, new String[0], new int[0], Arrays.<ColumnIO>asList(this), Arrays.<ColumnIO>asList(this));
  }

  void setLeaves(List<PrimitiveColumnIO> leaves) {
    this.leaves = leaves;
  }

  public List<PrimitiveColumnIO> getLeaves() {
    return this.leaves;
  }

  @Override
  public MessageType getType() {
    return (MessageType) super.getType();
  }
}
