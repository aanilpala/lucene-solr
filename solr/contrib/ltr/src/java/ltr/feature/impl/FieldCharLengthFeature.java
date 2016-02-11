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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;

import com.google.common.collect.Sets;

/**
 * This feature will return the character length of a stored field. If the field
 * is not available it will return a default score.
 *
 */
public class FieldCharLengthFeature extends Feature {
  String field;
  
  public FieldCharLengthFeature() {
    
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
    return new FieldCharLengthFeatureWeight(searcher, this.name, this.params,
        this.norm, this.id);
  }
  
  @Override
  public String toString() {
    return "FieldLengthFeature [field:" + this.field + "]";
  }
  
  public class FieldCharLengthFeatureWeight extends FeatureWeight {
    Set<String> fields = Sets.newHashSet();
    
    public FieldCharLengthFeatureWeight(final IndexSearcher searcher,
        final String name, final NamedParams params, final Normalizer norm,
        final int id) {
      super(searcher, name, params, norm, id);
      this.fields.add(FieldCharLengthFeature.this.field);
    }
    
    @Override
    public Query getQuery() {
      return FieldCharLengthFeature.this;
    }
    
    @Override
    public FeatureScorer scorer(final AtomicReaderContext context,
        final Bits acceptDocs) throws IOException {
      return new FieldCharLengthFeatureScorer(this, context);
      
    }
    
    public class FieldCharLengthFeatureScorer extends FeatureScorer {
      
      AtomicReaderContext context = null;
      
      public FieldCharLengthFeatureScorer(final FeatureWeight weight,
          final AtomicReaderContext context) throws IOException {
        super(weight);
        this.context = context;
      }
      
      @Override
      public float score() throws IOException {
        final Document doc = this.context.reader().document(this.docID,
            FieldCharLengthFeatureWeight.this.fields);
        if (doc != null) {
          final IndexableField idxF = doc
              .getField(FieldCharLengthFeature.this.field);
          if (idxF != null) {
            final String value = idxF.stringValue();
            if (value != null) {
              return value.length();
            }
          }
        }
        return FieldCharLengthFeature.this.defaultValue;
      }
      
      @Override
      public String toString() {
        return "FieldCharLengthFeature [name=" + this.name + " field="
            + FieldCharLengthFeature.this.field + "]";
      }
      
    }
  }
}
