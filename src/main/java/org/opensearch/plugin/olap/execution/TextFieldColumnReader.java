/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReader;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.index.fieldvisitor.FieldsVisitor;

/**
 * Reads text-field values for a set of doc IDs using Lucene stored fields and/or OpenSearch {@code
 * _source} — the two paths text fields actually have, since their doc values are empty.
 *
 * <p>One reader per leaf segment, serving ALL requested text columns together. Per doc, we invoke a
 * single {@link FieldsVisitor} that captures both {@code _source} (if loaded) and any {@code
 * store:true} stored fields in one Lucene call — avoiding the O(columns × docs) cost of
 * decompressing the stored-fields block once per column.
 *
 * <p>Multi-valued text: OpenSearch text fields can be arrays. We take the first value and log at
 * debug when more exist — same behavior {@link DocValueColumnReader} has for keyword columns via
 * SortedSet (reads the first ord).
 *
 * <p>This reader is stateful per-batch: {@link #resetBatch} is called once before the batch's
 * per-doc loop to reset the per-doc cache; {@link #readString} is called per (doc, column).
 */
public final class TextFieldColumnReader {

  private static final Logger logger = LogManager.getLogger(TextFieldColumnReader.class);

  /** Cached per-doc visitor + parsed _source map, keyed by docId within the current batch. */
  private final Map<Integer, PerDocCache> cacheByDoc = new HashMap<>();

  /** Names of stored-fields columns the caller wants for this batch. */
  private final Set<String> storedFieldNames;

  /** Names of source-extracted columns the caller wants for this batch. */
  private final Set<String> sourceFieldNames;

  /** True when at least one column needs {@code _source} → visitor loads {@code _source}. */
  private final boolean needsSource;

  private LeafReader leafReader;

  public TextFieldColumnReader(Set<String> storedFieldNames, Set<String> sourceFieldNames) {
    this.storedFieldNames = storedFieldNames == null ? Set.of() : storedFieldNames;
    this.sourceFieldNames = sourceFieldNames == null ? Set.of() : sourceFieldNames;
    this.needsSource = !this.sourceFieldNames.isEmpty();
  }

  /** Bind the reader to a specific Lucene leaf segment. Resets per-doc cache from prior leaves. */
  public void open(LeafReader reader) {
    this.leafReader = reader;
    this.cacheByDoc.clear();
  }

  /** Clear the per-doc cache between batches (call at the start of each batch). */
  public void resetBatch() {
    this.cacheByDoc.clear();
  }

  /**
   * Read the value of {@code fieldName} for {@code docId}. Returns null for missing values.
   * Subsequent calls on the same doc reuse the cached {@link FieldsVisitor}.
   */
  public String readString(int docId, String fieldName) throws IOException {
    PerDocCache cache = cacheByDoc.get(docId);
    if (cache == null) {
      cache = loadDoc(docId);
      cacheByDoc.put(docId, cache);
    }
    if (storedFieldNames.contains(fieldName)) {
      return extractFromStored(cache, fieldName);
    }
    if (sourceFieldNames.contains(fieldName)) {
      return extractFromSource(cache, fieldName);
    }
    // Caller asked for a field this reader wasn't told about. Defensive null.
    return null;
  }

  // ---- internals ----

  private PerDocCache loadDoc(int docId) throws IOException {
    // One FieldsVisitor per doc captures _source (when requested) + any stored-field requests
    // in a single Lucene stored-fields decompression pass.
    FieldsVisitor visitor =
        new FieldsVisitor(needsSource) {
          // We need the visitor to accept every stored-field name in storedFieldNames, not just
          // the base _id/_routing that FieldsVisitor defaults to. Override needsField to
          // additionally accept anything in storedFieldNames.
          @Override
          public Status needsField(org.apache.lucene.index.FieldInfo fieldInfo) {
            if (storedFieldNames.contains(fieldInfo.name)) {
              return Status.YES;
            }
            return super.needsField(fieldInfo);
          }
        };
    leafReader.storedFields().document(docId, visitor);
    return new PerDocCache(visitor);
  }

  private String extractFromStored(PerDocCache cache, String fieldName) {
    Map<String, List<Object>> fields = cache.visitor.fields();
    if (fields == null) {
      return null;
    }
    List<Object> values = fields.get(fieldName);
    if (values == null || values.isEmpty()) {
      return null;
    }
    if (values.size() > 1) {
      logger.debug(
          "multi-valued stored text field {} (size={}), taking first value",
          fieldName,
          values.size());
    }
    Object first = values.get(0);
    return first == null ? null : first.toString();
  }

  private String extractFromSource(PerDocCache cache, String fieldName) {
    if (cache.sourceMap == null) {
      BytesReference src = cache.visitor.source();
      if (src == null) {
        return null;
      }
      // Pass null so XContentHelper auto-detects the media type from the stored bytes.
      // OpenSearch preserves the original XContent format (JSON/CBOR/SMILE) for _source, so
      // hard-coding JSON here would fail on indices ingested in other formats.
      cache.sourceMap = XContentHelper.convertToMap(src, /* ordered */ true, (MediaType) null).v2();
    }
    Object raw = XContentMapValues.extractValue(fieldName, cache.sourceMap);
    if (raw == null) {
      return null;
    }
    if (raw instanceof List) {
      List<?> l = (List<?>) raw;
      if (l.isEmpty()) {
        return null;
      }
      if (l.size() > 1) {
        logger.debug(
            "multi-valued _source text field {} (size={}), taking first value",
            fieldName,
            l.size());
      }
      Object first = l.get(0);
      return first == null ? null : first.toString();
    }
    return raw.toString();
  }

  /** Per-doc cache: one FieldsVisitor, plus the lazily-parsed _source map. */
  private static final class PerDocCache {
    final FieldsVisitor visitor;
    Map<String, Object> sourceMap;

    PerDocCache(FieldsVisitor visitor) {
      this.visitor = visitor;
    }
  }
}
