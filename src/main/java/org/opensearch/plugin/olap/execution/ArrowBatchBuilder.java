/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.index.LeafReader;
import org.opensearch.plugin.olap.execution.DocValueColumnReader.DocValueType;

/**
 * Builds Arrow VectorSchemaRoot batches from Lucene doc values.
 *
 * <p>Reads doc values column by column for a batch of document IDs, populating Arrow vectors. The
 * resulting VectorSchemaRoot can be fed directly into velox4j's ExternalStream via the Arrow C Data
 * Interface.
 */
public class ArrowBatchBuilder {

  private static final int DEFAULT_BATCH_SIZE = 4096;

  private final BufferAllocator allocator;
  private final List<ColumnSpec> columns;
  private final int batchSize;

  public ArrowBatchBuilder(BufferAllocator allocator, List<ColumnSpec> columns) {
    this(allocator, columns, DEFAULT_BATCH_SIZE);
  }

  public ArrowBatchBuilder(BufferAllocator allocator, List<ColumnSpec> columns, int batchSize) {
    this.allocator = allocator;
    this.columns = columns;
    this.batchSize = batchSize;
  }

  /** Column specification: field name, Arrow type, and DocValue type. */
  public static class ColumnSpec {
    private final String name;
    private final ArrowType arrowType;
    private final DocValueType docValueType;

    public ColumnSpec(String name, ArrowType arrowType, DocValueType docValueType) {
      this.name = name;
      this.arrowType = arrowType;
      this.docValueType = docValueType;
    }

    public String getName() {
      return name;
    }

    public ArrowType getArrowType() {
      return arrowType;
    }

    public DocValueType getDocValueType() {
      return docValueType;
    }
  }

  /**
   * Read a batch of documents from the given leaf reader (contiguous range).
   *
   * @param reader Lucene leaf reader for one segment
   * @param startDoc First document ID in this batch
   * @param endDoc One past the last document ID
   * @return VectorSchemaRoot with populated Arrow vectors
   */
  public VectorSchemaRoot buildBatch(LeafReader reader, int startDoc, int endDoc)
      throws IOException {

    int numDocs = Math.min(endDoc - startDoc, batchSize);
    int[] docIds = new int[numDocs];
    for (int i = 0; i < numDocs; i++) {
      docIds[i] = startDoc + i;
    }
    return buildBatch(reader, docIds, numDocs);
  }

  /**
   * Read a batch of documents at arbitrary (sparse) doc ID positions. Used by predicate pushdown
   * where a Lucene query pre-filters doc IDs and only matching documents need to be read.
   *
   * @param reader Lucene leaf reader for one segment
   * @param docIds array of document IDs to read (need not be contiguous)
   * @param count number of valid entries in the docIds array
   * @return VectorSchemaRoot with populated Arrow vectors
   */
  public VectorSchemaRoot buildBatch(LeafReader reader, int[] docIds, int count)
      throws IOException {

    // Create Arrow schema and vectors
    List<Field> fields = new ArrayList<>(columns.size());
    for (ColumnSpec col : columns) {
      fields.add(new Field(col.name, FieldType.nullable(col.arrowType), null));
    }
    Schema schema = new Schema(fields);
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.setRowCount(count);

    // Open doc value readers for each column
    List<DocValueColumnReader> readers = new ArrayList<>(columns.size());
    for (ColumnSpec col : columns) {
      DocValueColumnReader dvReader = new DocValueColumnReader(col.name, col.docValueType);
      dvReader.open(reader);
      readers.add(dvReader);
    }

    // Populate each vector
    for (int col = 0; col < columns.size(); col++) {
      ColumnSpec spec = columns.get(col);
      DocValueColumnReader dvReader = readers.get(col);
      FieldVector vector = root.getVector(col);
      vector.allocateNew();

      for (int row = 0; row < count; row++) {
        populateValue(vector, row, docIds[row], dvReader, spec.arrowType);
      }

      vector.setValueCount(count);
    }

    return root;
  }

  private void populateValue(
      FieldVector vector, int row, int docId, DocValueColumnReader reader, ArrowType arrowType)
      throws IOException {
    if (arrowType instanceof ArrowType.Bool) {
      BitVector bv = (BitVector) vector;
      if (reader.hasValue(docId)) {
        bv.set(row, (int) reader.readLong(docId));
      }
    } else if (arrowType instanceof ArrowType.Int) {
      ArrowType.Int intType = (ArrowType.Int) arrowType;
      switch (intType.getBitWidth()) {
        case 8:
          TinyIntVector tiv = (TinyIntVector) vector;
          tiv.set(row, (byte) reader.readInt(docId));
          break;
        case 16:
          SmallIntVector siv = (SmallIntVector) vector;
          siv.set(row, (short) reader.readInt(docId));
          break;
        case 32:
          IntVector iv = (IntVector) vector;
          iv.set(row, reader.readInt(docId));
          break;
        case 64:
          BigIntVector biv = (BigIntVector) vector;
          biv.set(row, reader.readLong(docId));
          break;
      }
    } else if (arrowType instanceof ArrowType.FloatingPoint) {
      ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
      switch (fpType.getPrecision()) {
        case SINGLE:
          Float4Vector f4v = (Float4Vector) vector;
          f4v.set(row, reader.readFloat(docId));
          break;
        case DOUBLE:
          Float8Vector f8v = (Float8Vector) vector;
          f8v.set(row, reader.readDouble(docId));
          break;
      }
    } else if (arrowType instanceof ArrowType.Utf8) {
      VarCharVector vcv = (VarCharVector) vector;
      String value = reader.readString(docId);
      if (value != null) {
        vcv.set(row, value.getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  public int getBatchSize() {
    return batchSize;
  }
}
