package ltr.model.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import ltr.feature.FeatureStore;
import ltr.ranking.Feature;
import ltr.ranking.Feature.FeatureWeight;
import ltr.ranking.Feature.FeatureWeight.FeatureScorer;
import ltr.util.FeatureException;
import ltr.util.ModelException;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a stubbed reranking model that will be used only for computing the features.
 **/
public class LoggingModel extends Model {
  
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles
      .lookup().lookupClass());
  FeatureStore store = null;
  
  public LoggingModel(final FeatureStore store) throws FeatureException {
    this.store = store;
  }
  
  @Override
  public Weight createWeight(final IndexSearcher searcher) throws IOException {
    // FIXME: Each model should own its list of global features for logging, in
    // case different models
    // want to log different sets of features.
    final Collection<Feature> features = this.store.getFeatures();
    final List<Feature> modelFeatures = Collections.emptyList();
    return new LoggingModelWeight(searcher, this.getWeights(modelFeatures,
        searcher), this.getWeights(features, searcher));
  }
  
  public class LoggingModelWeight extends ModelWeight {
    
    public LoggingModelWeight(final IndexSearcher searcher,
        final FeatureWeight[] modelFeatures, final FeatureWeight[] allFeatures) {
      super(searcher, modelFeatures, allFeatures);
    }
    
    @Override
    protected ModelScorer makeModelScorer(final ModelWeight weight,
        final FeatureScorer[] featureScorers) {
      return new LoggingModelScorer(weight, featureScorers);
    }
    
    public class LoggingModelScorer extends ModelScorer {
      
      protected LoggingModelScorer(final Weight weight,
          final FeatureScorer[] featureScorers) {
        super(weight, featureScorers);
      }
      
      @Override
      public float score() throws IOException {
        {
          for (int i = 0; i < this.allFeatureScorers.length; i++) {
            final FeatureScorer scorer = this.allFeatureScorers[i];
            if (scorer == null) {
              // a scorer could be null in case of error, return the default
              // value if is null
              return LoggingModelWeight.this.defaultValues[i];
            }
            if (scorer.docID() != this.doc) {
              LoggingModelWeight.this.allFeatureValues[i] = scorer
                  .getDefaultValue();
            } else {
              try {
                LoggingModelWeight.this.allFeatureValues[i] = scorer.score();
              } catch (final Exception e) {
                LoggingModelWeight.this.allFeatureValues[i] = scorer
                    .getDefaultValue();
                logger.debug("error computing feature {}, \n{}",
                    LoggingModelWeight.this.allFeatureNames[i], e);
              }
            }
          }
          
          return 1;
        }
      }
      
      @Override
      public IntervalIterator intervals(final boolean collectIntervals)
          throws IOException {
        throw new UnsupportedOperationException();
      }
      
    }
    
  }
  
  @Override
  public Model replicate() throws ModelException {
    throw new UnsupportedOperationException();
  }
  
  @Override
  protected ModelWeight makeModelWeight(final IndexSearcher searcher,
      final FeatureWeight[] modelFeatures, final FeatureWeight[] allFeatures) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public FeatureStore getFeatureStore() {
    return this.store;
  }
  
}
