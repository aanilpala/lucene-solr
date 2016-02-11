package ltr.feature.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Set;

import ltr.feature.norm.Normalizer;
import ltr.ranking.Feature;
import ltr.util.CommonLtrParams;
import ltr.util.FeatureException;
import ltr.util.NamedParams;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SmallFloat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * This feature will return the length in terms of a field, base on its norm
 * field. If the norm is not available it will throw an exception when solr is
 * started.
 *
 */
public class FieldLengthFeature extends Feature {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles
      .lookup().lookupClass());

  String field;

  /** Cache of decoded bytes. */

  private static final float[] NORM_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++) {
      NORM_TABLE[i] = SmallFloat.byte315ToFloat((byte) i);
    }
  }

  /**
   * Decodes the norm value, assuming it is a single byte.
   */
  private final float decodeNorm(final long norm) {
    return NORM_TABLE[(int) (norm & 0xFF)]; // & 0xFF maps negative bytes to
    // positive above 127
  }

  public FieldLengthFeature() {

  }

  @Override
  public void init(final String name, final NamedParams params, final int id,
      final float defaultValue) throws FeatureException {
    super.init(name, params, id, defaultValue);
    if (!params.containsKey(CommonLtrParams.FIELD)) {
      throw new FeatureException("missing param " + CommonLtrParams.FIELD);
    }
  }

  @Override
  public FeatureWeight createWeight(final IndexSearcher searcher)
      throws IOException {
    this.field = (String) this.params.get(CommonLtrParams.FIELD);
    return new FieldLengthFeatureWeight(searcher, this.name, this.params,
        this.norm, this.id);
  }

  @Override
  public String toString() {
    return "FieldLengthFeature [field:" + this.field + "]";

  }

  public class FieldLengthFeatureWeight extends FeatureWeight {

    public FieldLengthFeatureWeight(final IndexSearcher searcher,
        final String name, final NamedParams params, final Normalizer norm,
        final int id) {
      super(searcher, name, params, norm, id);
    }

    @Override
    public Query getQuery() {
      return FieldLengthFeature.this;
    }

    @Override
    public FeatureScorer scorer(final AtomicReaderContext context,
        final Bits acceptDocs) throws IOException {
      return new FieldLengthFeatureScorer(this, context);

    }

    public class FieldLengthFeatureScorer extends FeatureScorer {

      AtomicReaderContext context = null;
      NumericDocValues norms = null;
      Set<String> fields = Sets.newHashSet();

      public FieldLengthFeatureScorer(final FeatureWeight weight,
          final AtomicReaderContext context) throws IOException {
        super(weight);
        this.context = context;
        this.fields.add(FieldLengthFeature.this.field);
        this.norms = context.reader().getNormValues(
            FieldLengthFeature.this.field);

        final Document doc = FieldLengthFeatureWeight.this.searcher.doc(0);
        if (doc != null) {
          final IndexableField idxF = doc
              .getField(FieldLengthFeature.this.field);

          if ((idxF != null) && idxF.fieldType().omitNorms()) {
            // if there are problems we don't want to log a line for each
            // document
            logger
            .debug(
                "FieldLengthFeatures can't be used if omitNorms is enabled (field={})",
                FieldLengthFeature.this.field);
          }
        }
        // In the constructor, docId is -1, so using 0 as default lookup
        final IndexableField idxF = FieldLengthFeatureWeight.this.searcher.doc(
            0).getField(FieldLengthFeature.this.field);
        if (idxF.fieldType().omitNorms()) {
          logger
          .debug(
              "FieldLengthFeatures can't be used if omitNorms is enabled (field={})",
              FieldLengthFeature.this.field);
        }
      }

      @Override
      public float score() throws IOException {
        final Document d = this.context.reader().document(this.docID,
            this.fields);
        final IndexableField idxF = d.getField(FieldLengthFeature.this.field);
        final float boost = idxF.boost();
        final long encodedNorm = this.norms.get(this.docID);
        final float norm = FieldLengthFeature.this.decodeNorm(encodedNorm);
        final float numTerms = (float) Math.pow(boost / norm, 2);
        return numTerms;
      }

      @Override
      public String toString() {
        return "FieldLengthFeature [name=" + this.name + " field="
            + FieldLengthFeature.this.field + "]";
      }

    }
  }

}
