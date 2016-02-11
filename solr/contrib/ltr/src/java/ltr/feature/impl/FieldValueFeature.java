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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * This feature will return a numerical/boolean value contained in a document
 * field. If the value is not available it will return a default score.
 */
public class FieldValueFeature extends Feature {
  String field;
  
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles
      .lookup().lookupClass());
  
  public FieldValueFeature() {
    
  }
  
  @Override
  public void init(final String name, final NamedParams params, final int id,
      final float defaultValue) throws FeatureException {
    super.init(name, params, id, defaultValue);
    this.field = (String) params.get(CommonLtrParams.FIELD);
    if (!params.containsKey(CommonLtrParams.FIELD)) {
      logger.error("no field {} in the feature declaration",
          CommonLtrParams.FIELD);
      throw new FeatureException("missing param " + CommonLtrParams.FIELD);
    }
  }
  
  @Override
  public FeatureWeight createWeight(final IndexSearcher searcher)
      throws IOException {
    
    return new FieldValueFeatureWeight(searcher, this.name, this.params,
        this.norm, this.id);
  }
  
  @Override
  public String toString() {
    return "DocValueFeature [field:" + this.field + "]";
    
  }
  
  public class FieldValueFeatureWeight extends FeatureWeight {
    
    Set<String> fields = Sets.newHashSet();
    
    public FieldValueFeatureWeight(final IndexSearcher searcher,
        final String name, final NamedParams params, final Normalizer norm,
        final int id) {
      super(searcher, name, params, norm, id);
      this.fields.add(FieldValueFeature.this.field);
    }
    
    @Override
    public Query getQuery() {
      return FieldValueFeature.this;
    }
    
    @Override
    public FeatureScorer scorer(final AtomicReaderContext context,
        final Bits acceptDocs) throws IOException {
      return new FieldValueFeatureScorer(this, context);
    }
    
    public class FieldValueFeatureScorer extends FeatureScorer {
      
      AtomicReaderContext context = null;
      
      public FieldValueFeatureScorer(final FeatureWeight weight,
          final AtomicReaderContext context) {
        super(weight);
        this.context = context;
      }
      
      @Override
      public float score() {
        try {
          final Document d = this.context.reader().document(this.docID,
              FieldValueFeatureWeight.this.fields);
          final IndexableField f = d.getField(FieldValueFeature.this.field);
          if (f == null) {
            logger.debug("no field {}", f);
            return FieldValueFeature.this.defaultValue;
          }
          final Number number = f.numericValue();
          if (number != null) {
            return number.floatValue();
          } else {
            final String string = f.stringValue();
            // boolean values in the index are encoded with the chars T/F
            if (string.equals("T")) {
              return 1;
            }
            if (string.equals("F")) {
              return 0;
            }
          }
        } catch (final IOException e) {}
        logger.debug("cannot convert to feature field {}",
            FieldValueFeature.this.field);
        return FieldValueFeature.this.defaultValue;
      }
      
      @Override
      public String toString() {
        return "FieldValueFeature [name=" + this.name + " fields="
            + FieldValueFeatureWeight.this.fields + "]";
      }
      
    }
    
  }
  
}
