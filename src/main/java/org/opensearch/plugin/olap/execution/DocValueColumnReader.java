/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.plugin.olap.execution;

import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;

/**
 * Reads a single column's values from Lucene DocValues for a leaf segment.
 *
 * <p>DocValues are the columnar storage in Lucene, ideal for analytical workloads.
 * Each field type maps to a specific DocValues type:
 * <ul>
 *   <li>long/int/short/byte → NumericDocValues or SortedNumericDocValues</li>
 *   <li>keyword/string → SortedDocValues or SortedSetDocValues</li>
 *   <li>float/double → NumericDocValues (encoded as long bits)</li>
 * </ul>
 */
public class DocValueColumnReader {

    private final String fieldName;
    private final DocValueType type;

    private NumericDocValues numericDocValues;
    private SortedDocValues sortedDocValues;
    private SortedNumericDocValues sortedNumericDocValues;
    private SortedSetDocValues sortedSetDocValues;

    public enum DocValueType {
        NUMERIC,
        SORTED,
        SORTED_NUMERIC,
        SORTED_SET
    }

    public DocValueColumnReader(String fieldName, DocValueType type) {
        this.fieldName = fieldName;
        this.type = type;
    }

    public void open(LeafReader reader) throws IOException {
        switch (type) {
            case NUMERIC:
                numericDocValues = DocValues.getNumeric(reader, fieldName);
                break;
            case SORTED:
                sortedDocValues = DocValues.getSorted(reader, fieldName);
                break;
            case SORTED_NUMERIC:
                sortedNumericDocValues = DocValues.getSortedNumeric(reader, fieldName);
                break;
            case SORTED_SET:
                sortedSetDocValues = DocValues.getSortedSet(reader, fieldName);
                break;
        }
    }

    /**
     * Read the long value for the given doc ID. Returns 0 if doc has no value.
     */
    public long readLong(int docId) throws IOException {
        switch (type) {
            case NUMERIC:
                if (numericDocValues.advanceExact(docId)) {
                    return numericDocValues.longValue();
                }
                return 0L;
            case SORTED_NUMERIC:
                if (sortedNumericDocValues.advanceExact(docId)) {
                    return sortedNumericDocValues.nextValue();
                }
                return 0L;
            default:
                throw new UnsupportedOperationException(
                    "Cannot read long from " + type + " for field " + fieldName
                );
        }
    }

    /**
     * Read the double value for the given doc ID (from numeric docvalues encoded as long bits).
     */
    public double readDouble(int docId) throws IOException {
        long bits = readLong(docId);
        return Double.longBitsToDouble(bits);
    }

    /**
     * Read the float value for the given doc ID (from numeric docvalues encoded as int bits).
     */
    public float readFloat(int docId) throws IOException {
        long bits = readLong(docId);
        return Float.intBitsToFloat((int) bits);
    }

    /**
     * Read the int value for the given doc ID.
     */
    public int readInt(int docId) throws IOException {
        return (int) readLong(docId);
    }

    /**
     * Read the string value for the given doc ID.
     */
    public String readString(int docId) throws IOException {
        switch (type) {
            case SORTED:
                if (sortedDocValues.advanceExact(docId)) {
                    return sortedDocValues.lookupOrd(sortedDocValues.ordValue())
                        .utf8ToString();
                }
                return null;
            case SORTED_SET:
                if (sortedSetDocValues.advanceExact(docId)) {
                    if (sortedSetDocValues.docValueCount() > 0) {
                        long ord = sortedSetDocValues.nextOrd();
                        return sortedSetDocValues.lookupOrd(ord).utf8ToString();
                    }
                }
                return null;
            default:
                throw new UnsupportedOperationException(
                    "Cannot read string from " + type + " for field " + fieldName
                );
        }
    }

    /**
     * Check if the given doc ID has a value for this field.
     */
    public boolean hasValue(int docId) throws IOException {
        switch (type) {
            case NUMERIC:
                return numericDocValues.advanceExact(docId);
            case SORTED:
                return sortedDocValues.advanceExact(docId);
            case SORTED_NUMERIC:
                return sortedNumericDocValues.advanceExact(docId);
            case SORTED_SET:
                return sortedSetDocValues.advanceExact(docId);
            default:
                return false;
        }
    }

    public String getFieldName() {
        return fieldName;
    }

    public DocValueType getType() {
        return type;
    }
}
