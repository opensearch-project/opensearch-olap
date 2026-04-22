/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap.execution;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;

/**
 * Lucene {@link Query} that keeps only docs whose value of {@code fieldName} passes a bloom filter
 * probe. Used as the probe-side pushdown for BLOOM runtime filters, replacing the earlier
 * feeder-layer per-doc predicate.
 *
 * <p>Because bloom filters are probabilistic by design, the scorer can't use a point/term index to
 * narrow the iterator set. It walks all docs with a value for the field (via a {@link
 * DocIdSetIterator} over doc values) and matches only those whose encoded value passes {@link
 * OlapBloomFilter#mightContain}. The hash-join above re-verifies the join key, so BLOOM false
 * positives surface as extra rows pushed into Velox but never as wrong results.
 *
 * <p><b>Missing-field semantics</b>: if the field is absent from a segment (no doc values), the
 * scorer is null — no docs match. This is stricter than the previous feeder-level predicate, which
 * returned {@code true} for missing values ("pass-through"). The hash-join still re-verifies keys,
 * so correctness is preserved: a doc without the join key can't match the join anyway.
 */
public final class BloomFilterQuery extends Query {

  private final String fieldName;
  private final String fieldType;
  private final OlapBloomFilter bloom;

  public BloomFilterQuery(String fieldName, String fieldType, OlapBloomFilter bloom) {
    this.fieldName = Objects.requireNonNull(fieldName);
    this.fieldType = Objects.requireNonNull(fieldType);
    this.bloom = Objects.requireNonNull(bloom);
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getFieldType() {
    return fieldType;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    return new ConstantScoreWeight(this, boost) {
      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        Scorer scorer = buildScorer(this, context);
        if (scorer == null) {
          return null;
        }
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) {
            return scorer;
          }

          @Override
          public long cost() {
            return scorer.iterator().cost();
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        // Bloom content is unique per query — caching across queries would be wrong.
        return false;
      }
    };
  }

  private Scorer buildScorer(Weight weight, LeafReaderContext context) throws IOException {
    TwoPhaseIterator twoPhase = buildTwoPhase(context);
    if (twoPhase == null) return null;
    return new ConstantScoreScorer(0f, ScoreMode.COMPLETE_NO_SCORES, twoPhase);
  }

  private TwoPhaseIterator buildTwoPhase(LeafReaderContext context) throws IOException {
    if ("keyword".equals(fieldType) || "text".equals(fieldType)) {
      SortedSetDocValues ss = DocValues.getSortedSet(context.reader(), fieldName);
      if (ss != null && ss.cost() > 0) {
        return keywordMultiValued(ss);
      }
      SortedDocValues s = DocValues.getSorted(context.reader(), fieldName);
      if (s != null && s.cost() > 0) {
        return keywordSingleValued(s);
      }
      return null;
    }
    if ("integer".equals(fieldType) || "long".equals(fieldType)) {
      SortedNumericDocValues sn = DocValues.getSortedNumeric(context.reader(), fieldName);
      if (sn != null && sn.cost() > 0) {
        return numericMultiValued(sn);
      }
      NumericDocValues n = DocValues.getNumeric(context.reader(), fieldName);
      if (n != null && n.cost() > 0) {
        return numericSingleValued(n);
      }
      return null;
    }
    return null;
  }

  private TwoPhaseIterator keywordMultiValued(SortedSetDocValues dv) {
    return new TwoPhaseIterator(dv) {
      @Override
      public boolean matches() throws IOException {
        int cnt = dv.docValueCount();
        for (int i = 0; i < cnt; i++) {
          long ord = dv.nextOrd();
          BytesRef v = dv.lookupOrd(ord);
          if (bloom.mightContain(v)) return true;
        }
        return false;
      }

      @Override
      public float matchCost() {
        return 5.0f;
      }
    };
  }

  private TwoPhaseIterator keywordSingleValued(SortedDocValues dv) {
    return new TwoPhaseIterator(dv) {
      @Override
      public boolean matches() throws IOException {
        BytesRef v = dv.lookupOrd(dv.ordValue());
        return bloom.mightContain(v);
      }

      @Override
      public float matchCost() {
        return 3.0f;
      }
    };
  }

  private TwoPhaseIterator numericMultiValued(SortedNumericDocValues dv) {
    return new TwoPhaseIterator(dv) {
      @Override
      public boolean matches() throws IOException {
        int cnt = dv.docValueCount();
        for (int i = 0; i < cnt; i++) {
          BytesRef enc = OlapBloomFilter.encodeKey(dv.nextValue(), fieldType);
          if (enc != null && bloom.mightContain(enc)) return true;
        }
        return false;
      }

      @Override
      public float matchCost() {
        return 5.0f;
      }
    };
  }

  private TwoPhaseIterator numericSingleValued(NumericDocValues dv) {
    return new TwoPhaseIterator(dv) {
      @Override
      public boolean matches() throws IOException {
        BytesRef enc = OlapBloomFilter.encodeKey(dv.longValue(), fieldType);
        return enc != null && bloom.mightContain(enc);
      }

      @Override
      public float matchCost() {
        return 3.0f;
      }
    };
  }

  @Override
  public String toString(String field) {
    return "BloomFilterQuery(field="
        + fieldName
        + ",type="
        + fieldType
        + ",hashCount="
        + bloom.hashCount()
        + ",bits="
        + bloom.setSizeBits()
        + ")";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(fieldName)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof BloomFilterQuery)) return false;
    BloomFilterQuery other = (BloomFilterQuery) o;
    return fieldName.equals(other.fieldName)
        && fieldType.equals(other.fieldType)
        && bloom == other.bloom; // identity — bloom is per-query, not expected to dedupe
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + fieldName.hashCode();
    h = 31 * h + fieldType.hashCode();
    h = 31 * h + System.identityHashCode(bloom);
    return h;
  }
}
