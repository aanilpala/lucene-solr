package ltr.feature.impl;

import java.io.IOException;
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

import com.google.common.collect.Sets;

/**
 * This feature will return the number of values in a specified multivalued field.
 * Float.MAXVALUE in case of error.
 */
public class MultivaluedFieldLengthFeature extends Feature {
  private String field;

  public MultivaluedFieldLengthFeature() {

  }

  @Override
  public void init(final String name, final NamedParams params, final int id,final float defaultValue) throws FeatureException {
    super.init(name, params, id, defaultValue);
    if (!params.containsKey(CommonLtrParams.FIELD)) {
      throw new FeatureException("missing param" + CommonLtrParams.FIELD);
    }
  }

  @Override
  public FeatureWeight createWeight(final IndexSearcher searcher) throws IOException {
    this.field = (String) this.params.get(CommonLtrParams.FIELD);
    return new FieldLengthFeatureWeight(searcher, this.name, this.params, this.norm, this.id);
  }

  @Override
  public String toString() {
    return "MultivaluedFieldLengthFeature [field:" + this.field + "]";

  }

  public class FieldLengthFeatureWeight extends FeatureWeight {

    public FieldLengthFeatureWeight(final IndexSearcher searcher, final String name, final NamedParams params, final Normalizer norm, final int id) {
      super(searcher, name, params, norm, id);
    }

    @Override
    public Query getQuery() {
      return MultivaluedFieldLengthFeature.this;
    }

    @Override
    public FeatureScorer scorer(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
      return new FieldLengthFeatureScorer(this, context);

    }

    public class FieldLengthFeatureScorer extends FeatureScorer {

      private final AtomicReaderContext context;
      private NumericDocValues norms;
      private final Set<String> fields = Sets.newHashSet();

      public FieldLengthFeatureScorer(final FeatureWeight weight, final AtomicReaderContext context) throws IOException {
        super(weight);
        this.context = context;
        this.fields.add(MultivaluedFieldLengthFeature.this.field);
      }

      @Override
      public float score() throws IOException {

        final Document doc = this.context.reader().document(this.docID, this.fields);
        final IndexableField[] idxf = doc.getFields(MultivaluedFieldLengthFeature.this.field);
        if (idxf == null) {
          return MultivaluedFieldLengthFeature.this.defaultValue;
        }
        return idxf.length;
      }

      @Override
      public String toString() {
        return "MultivaluedFieldLengthFeature [name=" + this.name + " field=" + MultivaluedFieldLengthFeature.this.field + "]";
      }

    }
  }

}
