package ltr.feature.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import ltr.feature.norm.Normalizer;
import ltr.ranking.Feature;
import ltr.util.NamedParams;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This feature will return the score of the original query performed on solr.
 */
public class OriginalScoreFeature extends Feature {
  
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles
      .lookup().lookupClass());
  
  @Override
  public OriginalScoreWeight createWeight(final IndexSearcher searcher) throws IOException {
    return new OriginalScoreWeight(searcher, this.name, this.params, this.norm, this.id);
    
  }
  
  public class OriginalScoreWeight extends FeatureWeight {
    
    private Weight w;
    
    public OriginalScoreWeight(final IndexSearcher searcher, final String name, final NamedParams params, final Normalizer norm, final int id) {
      super(searcher, name, params, norm, id);
      
    }

    @Override
    public void setOriginalQuery(Query originalQuery) throws IOException  {
      // I can't set w before in the constructor because I would need to have it
      // in the query for doing that. But the query/feature is shared among
      // different threads
      // so I can't set the original query there.
      if (originalQuery == null){
        logger.warn("cannot find original query ");
        originalQuery = new MatchAllDocsQuery();
      }
      this.w = this.searcher.createNormalizedWeight(originalQuery);
    }
    
    @Override
    public void process() throws IOException {
      // different threads so I can't set the original query there.
      this.w = this.searcher.createNormalizedWeight(this.originalQuery);
    };
    
    @Override
    public Query getQuery() {
      return OriginalScoreFeature.this;
    }
    
    @Override
    public Explanation explain(final AtomicReaderContext context, final int doc) throws IOException {
      // Explanation e = w.explain(context, doc);
      final Scorer s = this.w.scorer(context, PostingFeatures.DOCS_AND_FREQS, context.reader().getLiveDocs());
      s.advance(doc);
      final float score = s.score();
      return new Explanation(score, "original score query: " + this.originalQuery);
    }
    
    @Override
    public FeatureScorer scorer(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
      
      final Scorer originalScorer = this.w.scorer(context, PostingFeatures.DOCS_AND_FREQS, acceptDocs);
      return new OriginalScoreScorer(this, originalScorer);
    }
    
    public class OriginalScoreScorer extends FeatureScorer {
      private final Scorer originalScorer;
      
      public OriginalScoreScorer(final FeatureWeight weight, final Scorer originalScorer) {
        super(weight);
        this.originalScorer = originalScorer;
      }
      
      @Override
      public int advance(final int target) throws IOException {
        this.docID = this.originalScorer.advance(target);
        return this.docID;
      }
      
      @Override
      public float score() throws IOException {
        if (OriginalScoreWeight.this.originalQuery instanceof MatchAllDocsQuery){
          return OriginalScoreFeature.this.defaultValue;
        }
        final float score = this.originalScorer.score();
        return score;
      }
      
      @Override
      public String toString() {
        return "OriginalScoreFeature [query:" + OriginalScoreWeight.this.originalQuery.toString() + "]";
      }
    }
    
  }
  
}
