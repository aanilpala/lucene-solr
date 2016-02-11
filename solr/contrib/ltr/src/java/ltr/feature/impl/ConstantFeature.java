package ltr.feature.impl;

import java.io.IOException;

import ltr.feature.norm.Normalizer;
import ltr.ranking.Feature;
import ltr.util.CommonLtrParams;
import ltr.util.NamedParams;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;

/**
 * This feature will produce a constant value for each document.
 * The value can be passed by setting the param "value" into the configuration.
 */
public class ConstantFeature extends Feature {

  float value = -1f;

  public ConstantFeature() {

  }
  
  

  /**
   * @return the value
   */
  public float getValue() {
    return this.value;
  }



  /**
   * @param value the value to set
   */
  public void setValue(final float value) {
    this.value = value;
  }



  @Override
  public FeatureWeight createWeight(final IndexSearcher searcher) throws IOException {
    this.value = this.params.getFloat(CommonLtrParams.VALUE, this.value);
    return new ConstantFeatureWeight(searcher, this.name, this.params, this.norm, this.id);
  }

  public class ConstantFeatureWeight extends FeatureWeight {

    public ConstantFeatureWeight(final IndexSearcher searcher, final String name, final NamedParams params, final Normalizer norm, final int id) {
      super(searcher, name, params, norm, id);
    }

    @Override
    public Query getQuery() {
      return ConstantFeature.this;
    }

    @Override
    public FeatureScorer scorer(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
      return new ConstantFeatureScorer(this, ConstantFeature.this.value, "ConstantFeature");
    }

  }

}
