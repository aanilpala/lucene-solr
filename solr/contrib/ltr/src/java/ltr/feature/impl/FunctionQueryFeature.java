package ltr.feature.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import ltr.feature.norm.Normalizer;
import ltr.ranking.Feature;
import ltr.util.FeatureException;
import ltr.util.NamedParams;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows to use a function query as a feature. In case of error the feature
 * will always produce the default value FIXME put example.
 */
public class FunctionQueryFeature extends Feature {
  
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles
      .lookup().lookupClass());
  
  @Override
  public FeatureWeight createWeight(final IndexSearcher searcher)
      throws IOException {
    return new FunctionQueryWeight(searcher, this.name, this.params, this.norm,
        this.id);
  }
  
  public class FunctionQueryWeight extends FeatureWeight {
    
    public FunctionQueryWeight(final IndexSearcher searcher, final String name,
        final NamedParams params, final Normalizer norm, final int id) {
      super(searcher, name, params, norm, id);
    }
    
    @Override
    public Query getQuery() {
      return FunctionQueryFeature.this;
    }
    
    @Override
    public FeatureScorer scorer(final AtomicReaderContext context,
        final Bits acceptDocs) throws IOException {
      final String fq = (String) this.getParams().get(CommonParams.FQ);
      Query query;
      final FunctionQParser parser = new FunctionQParser(fq, null, null,
          this.request);
      try {
        query = parser.parse();
      } catch (final SyntaxError e) {
        logger.error("cannot produce query feature {}",e);
        throw new FeatureException(e.getMessage());
      }
      Weight w = null;
      query.setBoost(1);
      w = query.createWeight(this.searcher);
      w.getValueForNormalization();
      
      final Scorer functionScorer = w.scorer(context,
          Weight.PostingFeatures.DOCS_AND_FREQS, acceptDocs);
      
      return new FunctionQueryFeatureScorer(this, functionScorer);
    }
    
    public class FunctionQueryFeatureScorer extends FeatureScorer {
      private String fq = "";
      private final Scorer functionScorer;
      
      public FunctionQueryFeatureScorer(final FeatureWeight weight,
          final Scorer functionScorer) {
        super(weight);
        this.fq = (String) FunctionQueryWeight.this.params.get(CommonParams.FQ);
        this.functionScorer = functionScorer;
      }
      
      @Override
      public int advance(final int target) throws IOException {
        this.docID = this.functionScorer.advance(target);
        return this.docID;
      }
      
      @Override
      public float score() throws IOException {
        return this.functionScorer.score();
      }
      
      @Override
      public String toString() {
        return "FunctionQueryFeature [function:" + this.fq + "]";
      }
    }
    
  }
  
}
