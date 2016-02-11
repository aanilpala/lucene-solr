package ltr.ranking;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;

import ltr.feature.FeatureStore;
import ltr.feature.LocalFeatureStores;
import ltr.log.FeatureLogger;
import ltr.model.impl.LoggingModel;
import ltr.model.impl.Model;
import ltr.model.impl.Model.ModelWeight;
import ltr.util.CommonLtrParams;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Weight.PostingFeatures;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformContext;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.response.transform.TransformerWithContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This transformer will take care to generate and append in the response the
 * features declared in the feature store of the current model. The class is
 * useful if you are not interested in the reranking (e.g., bootstrapping a
 * machine learning framework).
 */

public class LTRFeatureLoggerTransformerFactory extends TransformerFactory {
  
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles
      .lookup().lookupClass());
  
  private final LocalFeatureStores stores = new LocalFeatureStores();
  
  @Override
  public void init(@SuppressWarnings("rawtypes") final NamedList args) {
    super.init(args);
  }
  
  @Override
  public DocTransformer create(final String name, final SolrParams params,
      final SolrQueryRequest req) {
    // this will be created at each request
    final SolrResourceLoader solrResourceLoader = req.getCore()
        .getResourceLoader();
    final IndexSearcher searcher = req.getSearcher();
    final String featureStoreName = params.get(
        CommonLtrParams.FEATURE_STORE_PARAM,
        CommonLtrParams.DEFAULT_FEATURE_STORE);
    Model featureStoreModel = null;
    try {
      
      final FeatureStore featureStore = this.stores
          .getFeatureStoreFromSolrConfigOrResources(featureStoreName,
              solrResourceLoader);
      featureStoreModel = new LoggingModel(featureStore);
    } catch (final Exception e) {
      logger.error("retrieving the feature store {}\n{}", featureStoreName, e);
      return new EmptyFeatureTrasformer(name);
    }
    return new FeatureTransformer(name, searcher, featureStoreModel);
  }
  
  class EmptyFeatureTrasformer extends TransformerWithContext {
    private final String name;
    
    public EmptyFeatureTrasformer(final String name) {
      this.name = name;
    }
    
    @Override
    public String getName() {
      return this.name;
    }
    
    @Override
    public void transform(final SolrDocument doc, final int docid)
        throws IOException {
      doc.addField(this.name, "cannot produce feature vector");
    }
    
  }
  
  class FeatureTransformer extends TransformerWithContext {
    
    private final String name;
    private List<AtomicReaderContext> leafContexts;
    private final Model reRankModel;
    private ModelWeight modelWeight;
    private FeatureLogger<?> featurelLogger;
    private final String featureStoreName;
    private final float featureStoreVersion;
    
    /**
     * @param name
     *          Name of the field to be added in a document representing the
     *          feature vectors
     */
    public FeatureTransformer(final String name, final IndexSearcher searcher,
        final Model model) {
      this.name = name;
      this.reRankModel = model;
      final FeatureStore featureStore = this.reRankModel.getFeatureStore();
      this.featureStoreName = featureStore.getStoreName();
      this.featureStoreVersion = featureStore.getVersion();
    }
    
    @Override
    public String getName() {
      return this.name;
    }
    
    @Override
    public void setContext(final TransformContext context) {
      super.setContext(context);
      if (context == null) {
        return;
      }
      if (context.req == null) {
        return;
      }
      
      if (this.reRankModel == null) {
        logger.error(CommonLtrParams.FEATURE_ERROR);
        return;
      }
      this.reRankModel.setRequest(context.req);
      this.reRankModel.setOriginalQuery(context.query);
      logger.info("query = {} ", context.query);
      this.featurelLogger = this.reRankModel.getFeatureLogger();
      final IndexSearcher searcher = context.searcher;
      if (searcher == null) {
        logger.error(CommonLtrParams.FEATURE_ERROR);
        return;
      }
      this.leafContexts = searcher.getTopReaderContext().leaves();
      Weight w = null;
      try {
        w = this.reRankModel.createWeight(searcher);
      } catch (final IOException e) {
        logger.error(CommonLtrParams.FEATURE_ERROR);
        return;
      }
      if ((w == null) || !(w instanceof ModelWeight)) {
        logger.error(CommonLtrParams.FEATURE_ERROR);
        return;
      }
      this.modelWeight = (ModelWeight) w;
    }
    
    @Override
    public void transform(final SolrDocument doc, final int docid)
        throws IOException {
      // Note: this is the entry point of the computation of the features.
      // at the moment features are computed here solr 4.8.1 does not support
      // reranking
      // name. so we cannot recompute the score for each document during the
      // reranking here.
      if (this.modelWeight == null) {
        doc.addField(this.name, CommonLtrParams.FEATURE_ERROR);
        return;
      }
      final int n = ReaderUtil.subIndex(docid, this.leafContexts);
      final AtomicReaderContext atomicContext = this.leafContexts.get(n);
      final int deBasedDoc = docid - atomicContext.docBase;
      final Scorer r = this.modelWeight.scorer(atomicContext,
          PostingFeatures.DOCS_AND_FREQS,null);
      
      if (r.advance(deBasedDoc) != deBasedDoc) {
        logger.info("cannot find doc {} = {}", docid, doc);
        doc.addField(this.name, this.featurelLogger.makeRecord(docid,
            this.featureStoreName, this.featureStoreVersion, new String[0],
            new float[0]));
      } else {
        final float finalScore = r.score();
        final String[] names = this.modelWeight.getAllFeatureNames();
        final float[] values = this.modelWeight.getAllFeatureValues();
        
        doc.addField(this.name, this.featurelLogger.makeRecord(docid,
            this.featureStoreName, this.featureStoreVersion, names, values));
      }
      
    }
    
  }
}
