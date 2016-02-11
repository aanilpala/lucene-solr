package ltr.model.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import ltr.feature.FeatureStore;
import ltr.feature.ModelMetadata;
import ltr.feature.impl.ConstantFeature;
import ltr.feature.norm.Normalizer;
import ltr.feature.norm.impl.IdentityNormalizer;
import ltr.log.FeatureLogger;
import ltr.ranking.Feature;
import ltr.ranking.Feature.FeatureWeight;
import ltr.ranking.Feature.FeatureWeight.FeatureScorer;
import ltr.util.ModelException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A learned model, given a document context and a list of features predict a
 * relevance score.
 *
 */
public abstract class Model extends Query {
  // A model will produce a score for each document given a query so
  // it is considered a scoring function, and extends the lucene Query object.

  // contains a description of the model
  protected ModelMetadata meta;

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles
      .lookup().lookupClass());

  // feature logger to output the features.
  private FeatureLogger<?> fl = FeatureLogger.getFeatureLogger(
      FeatureLogger.Format.CSV, 50);

  // Map of external parameters, such as query intent, that can be used by
  // features
  protected Map<String,String> efi;

  // Original solr query used to fetch matching documents
  protected Query originalQuery;

  // Original solr request
  protected SolrQueryRequest request;

  public void init(final ModelMetadata meta) throws ModelException {
    this.meta = meta;
  }

  // return an instance of this model
  public abstract Model replicate() throws ModelException;

  public ModelMetadata getMetadata() {
    return this.meta;
  }

  public abstract FeatureStore getFeatureStore();

  public void setFeatureLogger(final FeatureLogger<?> fl) {
    this.fl = fl;
  }

  public FeatureLogger<?> getFeatureLogger() {
    return this.fl;
  }

  public Collection<Feature> getAllFeatures() {
    return this.meta.getAllFeatures();
  }

  public void setOriginalQuery(final Query mainQuery) {
    this.originalQuery = mainQuery;
  }

  public void setExternalFeatureInfo(
      final Map<String,String> externalFeatureInfo) {
    this.efi = externalFeatureInfo;
  }

  public void setRequest(final SolrQueryRequest request) {
    this.request = request;
  }

  /**
   * This method will produce an {@link Explanation} describing how the final
   * score in the reranking was computed.
   *
   * @param context
   *          the atomic reader use to compute the features for this document
   * @param doc
   *          the current document
   * @param finalScore
   *          the final score predicted
   * @param featureExplanations
   *          a list of explanations, each one describing how a feature was
   *          computed
   * @return an object describing how the reranking score for this document was
   *         computed
   */
  public Explanation explain(final AtomicReaderContext context, final int doc,
      final float finalScore, final List<Explanation> featureExplanations) {
    final Explanation e = new Explanation(finalScore, this.meta.getName()
        + " [ " + this.meta.getType() + " ] model applied to features");
    for (final Explanation featureExplain : featureExplanations) {
      e.addDetail(featureExplain);
    }
    return e;

  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = (prime * result)
        + ((this.meta == null) ? 0 : this.meta.hashCode());
    result = (prime * result)
        + ((this.originalQuery == null) ? 0 : this.originalQuery.hashCode());
    result = (prime * result)
        + ((this.efi == null) ? 0 : this.originalQuery.hashCode());
    result = (prime * result) + this.toString().hashCode();
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    final Model other = (Model) obj;
    if (this.meta == null) {
      if (other.meta != null) {
        return false;
      }
    } else if (!this.meta.equals(other.meta)) {
      return false;
    }
    if (this.originalQuery == null) {
      if (other.originalQuery != null) {
        return false;
      }
    } else if (!this.originalQuery.equals(other.originalQuery)) {
      return false;
    }
    return true;
  }

  public SolrQueryRequest getRequest() {
    return this.request;
  }

  public List<Feature> getFeatures() {
    return this.meta.getFeatures();
  }

  @Override
  public Weight createWeight(final IndexSearcher searcher) throws IOException {
    final Collection<Feature> features = this.getAllFeatures();
    final List<Feature> modelFeatures = this.getFeatures();
    return this.makeModelWeight(searcher,
        this.getWeights(modelFeatures, searcher),
        this.getWeights(features, searcher));
  }

  protected abstract ModelWeight makeModelWeight(IndexSearcher searcher,
      FeatureWeight[] modelFeatures, FeatureWeight[] allFeatures);

  protected FeatureWeight[] getWeights(final Collection<Feature> features,
      final IndexSearcher searcher) throws IOException {
    final FeatureWeight[] featureWeights = new FeatureWeight[features.size()];
    final SolrQueryRequest req = this.getRequest();
    int featureId = 0;
    // since the feature store is a linkedhashmap order is preserved
    for (final Feature f : features) {
      try {
        featureWeights[featureId] = f.createWeight(searcher);
        featureWeights[featureId].setRequest(req);
        featureWeights[featureId].setOriginalQuery(this.originalQuery);
      } catch (final Exception e) {
        logger.error("computing weight for feature {} - {}", f.getName(), e);
        // in case of error we return always the same constant value for this feauture
        final ConstantFeature constantFeature = new ConstantFeature();
        constantFeature.setValue(f.getDefaultValue());
        featureWeights[featureId] = constantFeature.createWeight(searcher);
      }

      ++featureId;
    }
    return featureWeights;
  }

  @Override
  public String toString(final String field) {
    return field;
  }

  /**
   *
   * ModelWeight will take care to generate a ModelScorer in order to score a
   * given document (see {@link Weight}). This object will first generate all
   * the FeatureScorer that we need to compute the features for a document, and
   * it will then pass the features to the ModelScorer.
   */
  public abstract class ModelWeight extends Weight {

    // List of the model's features used for scoring. This is a subset of the
    // features used for logging.
    private final FeatureWeight[] modelFeatures;
    private final float[] modelFeatureValuesNormalized;
    protected final float[] defaultValues;

    // List of all the feature values, used for both scoring and logging
    private final FeatureWeight[] allFeatureWeights;
    final float[] allFeatureValues;
    final String[] allFeatureNames;

    /**
     * Builds a ModelWeight Object
     *
     * @param searcher
     *          the index
     * @param modelFeatures
     *          the features specified from the model in order to compute the
     *          reranking score
     * @param allFeatures
     *          a superset of modelFeatures, containing all the features
     *          declared in the feature store, computed in order to log them
     */
    public ModelWeight(final IndexSearcher searcher,
        final FeatureWeight[] modelFeatures, final FeatureWeight[] allFeatures) {
      this.allFeatureWeights = allFeatures;
      this.modelFeatures = modelFeatures;
      this.modelFeatureValuesNormalized = new float[modelFeatures.length];
      this.allFeatureValues = new float[allFeatures.length];
      this.defaultValues = new float[allFeatures.length];
      this.allFeatureNames = new String[allFeatures.length];

      for (int featureId = 0; featureId < allFeatures.length; ++featureId) {
        this.allFeatureNames[featureId] = allFeatures[featureId].getName();
        this.defaultValues[featureId] = allFeatures[featureId].getDefaultValue();
      }
    }

    /**
     * @return the allFeatureWeights
     */
    public FeatureWeight[] getAllFeatureWeights() {
      return this.allFeatureWeights;
    }

    /**
     * @return the allFeatureValues
     */
    public float[] getAllFeatureValues() {
      return this.allFeatureValues;
    }

    /**
     * @return the modelFeatureValuesNormalized
     */
    public float[] getModelFeatureValuesNormalized() {
      return this.modelFeatureValuesNormalized;
    }

    /**
     * @return the allFeatureNames
     */
    public String[] getAllFeatureNames() {
      return this.allFeatureNames;
    }

    /**
     * Goes through all the stored feature values, and calculates the normalized
     * values for all the features that will be used for scoring.
     */
    public void normalize() {
      int featureId = 0;
      for (final FeatureWeight feature : this.modelFeatures) {
        final Normalizer norm = feature.getNorm();
        this.modelFeatureValuesNormalized[featureId] = norm
            .normalize(this.allFeatureValues[feature.getId()]);
        featureId++;
      }
    }

    @Override
    public Explanation explain(final AtomicReaderContext context, final int doc)
        throws IOException {
      final FeatureScorer[] featureScorers = new FeatureScorer[this.allFeatureValues.length];

      final Explanation[] explanations = new Explanation[this.allFeatureValues.length];
      int featureId = 0;
      for (final FeatureWeight feature : this.allFeatureWeights) {
        featureScorers[featureId] = feature.scorer(context, null);
        explanations[featureId++] = feature.explain(context, doc);
      }

      final List<Explanation> featureExplanations = new ArrayList<>();
      for (final FeatureWeight f : this.modelFeatures) {
        final Normalizer n = f.getNorm();
        Explanation e = explanations[f.id];
        if (n != IdentityNormalizer.INSTANCE) {
          e = n.explain(e);
        }
        featureExplanations.add(e);
      }
      // TODO this calls twice the scorers, could be optimized.
      final ModelScorer bs = this.makeModelScorer(this, featureScorers);
      // diego: no need to advance, scorers here will be in the required
      // position
      bs.advance(doc);
      final float finalScore = bs.score();
      return Model.this.explain(context, doc, finalScore, featureExplanations);
    }

    @Override
    public Query getQuery() {
      return Model.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return 1;
    }

    @Override
    public void normalize(final float norm, final float topLevelBoost) {
      for (final FeatureWeight feature : this.allFeatureWeights) {
        feature.normalize(norm, topLevelBoost);
      }
    }

    @Override
    public Scorer scorer(final AtomicReaderContext context,
        final PostingFeatures features, final Bits acceptDocs)
            throws IOException {
      final FeatureScorer[] featureScorers = new FeatureScorer[this.allFeatureWeights.length];
      for (int featureId = 0; featureId < this.allFeatureWeights.length; featureId++) {
        try {
          featureScorers[featureId] = this.allFeatureWeights[featureId].scorer(context,
              acceptDocs);
        } catch (final Exception e) {
          // if scorer() throws an exception we set the scorer to null
          logger.error("computing feature {} ", this.allFeatureNames[featureId]);

          featureScorers[featureId] = null;
        }
      }
      return this.makeModelScorer(this, featureScorers);
    }

    protected abstract ModelScorer makeModelScorer(ModelWeight weight,
        FeatureScorer[] featureScorers);

    /**
     * A model scorer will take a list of {@link FeatureScorer} and it will
     * apply to a give document in order to generate the its feature values. A
     * concrete learning to rank scorer will have to extend this and implement
     * the method score() in order in order to access the feature values and
     * combine them in a final score. {@link LoggingModel}.
     */
    public abstract class ModelScorer extends Scorer {

      protected final FeatureScorer[] allFeatureScorers;

      /** The document number of the current match. */
      protected int doc = -1;

      protected ModelScorer(final Weight weight,
          final FeatureScorer featureScorers[]) {
        super(weight);
        this.allFeatureScorers = featureScorers;
      }

      @Override
      public long cost() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int docID() {
        return this.doc;
      }

      @Override
      public int advance(final int target) throws IOException {
        assert this.doc != NO_MORE_DOCS;
        this.doc = NO_MORE_DOCS;
        for (final FeatureScorer scorer : this.allFeatureScorers) {
          this.doc = Math.min(this.doc, scorer.advance(target));
        }
        return this.doc;
      }

      @Override
      public int freq() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int nextDoc() throws IOException {
        throw new UnsupportedOperationException();
      }

    }

  }

}
