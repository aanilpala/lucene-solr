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
 * This feature will return true if the current has the specified field,
 * otherwise false.
 */
public class HasFieldFeature extends Feature {
  private String field;
  private final Set<String> fields = Sets.newHashSet();
  
  public HasFieldFeature() {
    
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
    this.fields.add(this.field);
    return new HasFieldFeatureWeight(searcher, this.name, this.params,
        this.norm, this.id);
  }
  
  @Override
  public String toString() {
    return "DocValueFeature [field:" + this.field + "]";
    
  }
  
  public class HasFieldFeatureWeight extends FeatureWeight {
    
    public HasFieldFeatureWeight(final IndexSearcher searcher,
        final String name, final NamedParams params, final Normalizer norm,
        final int id) {
      super(searcher, name, params, norm, id);
    }
    
    @Override
    public Query getQuery() {
      return HasFieldFeature.this;
    }
    
    @Override
    public FeatureScorer scorer(final AtomicReaderContext context,
        final Bits acceptDocs) throws IOException {
      return new HasFieldFeatureScorer(this, context);
    }
    
    public class HasFieldFeatureScorer extends FeatureScorer {
      
      private final AtomicReaderContext context;
      
      public HasFieldFeatureScorer(final FeatureWeight weight,
          final AtomicReaderContext context) {
        super(weight);
        this.context = context;
      }
      
      @Override
      public float score() {
        try {
          final Document doc = this.context.reader().document(this.docID,
              HasFieldFeature.this.fields);
          final IndexableField f = doc.getField(HasFieldFeature.this.field);
          if (f == null) {
            return 0;
          }
          final int fieldLength = f.stringValue().length();
          if (fieldLength > 0) {
            return 1;
          }
          
          if (fieldLength == 0) {
            return 0;
          }
          
        } catch (final IOException e) {
          
        }
        return HasFieldFeature.this.defaultValue;
      }
      
      @Override
      public String toString() {
        return "HasFieldFeatureScorer [name=" + this.name + " field="
            + HasFieldFeature.this.fields + "]";
      }
      
    }
  }
  
}
