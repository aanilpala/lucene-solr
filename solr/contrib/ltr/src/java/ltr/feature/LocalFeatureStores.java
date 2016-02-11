package ltr.feature;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import ltr.feature.io.JsonFileFeatureLoader;

import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local feature stores are loaded from the resources. Each feature store will
 * be encoded in one json file, where the name of the file will match the name
 * of the feature store (e.g., nws.json will contain the features for the
 * feature store 'nws').
 *
 * Local feature stores will be located in the folder '$LOCAL_FEATURE_FOLDER'
 * inside src/resources
 *
 */
public class LocalFeatureStores {
  
  private final static String LOCAL_FEATURE_FOLDER = "features";
  // a cache with the loaded stores
  private final Map<String,FeatureStore> stores;
  // parse a json file and create a feature store
  private final JsonFileFeatureLoader loader;
  
  private static final Logger logger = LoggerFactory
      .getLogger(LocalFeatureStores.class);
  
  public LocalFeatureStores() {
    this.stores = new HashMap<>();
    this.loader = new JsonFileFeatureLoader();
  }
  
  /***
   * load a json encoded feature store given it's name and a reader on the json
   *
   * @param featureStoreName
   *          the name of the feature store
   * @param featureStoreReader
   *          a reader of the json encoded feature store
   * @return the feature store described in the configuration file. If there
   *         reading the features, returns an empty feature store.
   */
  private FeatureStore loadStore(final String featureStoreName,
      final Reader featureStoreReader) {
    final FeatureStore featureStore = new FeatureStore(featureStoreName);
    try {
      this.loader.loadFeatures(featureStoreReader, featureStore);
    } catch (final IOException e) {
      logger.error("loading the feature store {}: {}", featureStoreName, e);
    }
    return featureStore;
  }
  
  /**
   * Loads a json feature store located in the solr config folder
   * (in the directory LOCAL_FEATURE_FOLDER). The method will use the solrResourceLoader
   * so it will try to load the feature store in order from: <ol>
   * <li> inside config/ if path is not absolute</li>
   * <li> otherwise searches at path.getAbsoluteFile() [not relevant in our case]</li>
   * <li> uses the class loader to search [searches resources/ at this point]</li>
   * </ol>
   *
   * @param featureStoreName
   *          name of the feature store
   * @param solrResourceLoader
   *          the solr resource loader
   * @return the feature store described in the json file, or an empty feature
   *         store in case of error
   */
  public FeatureStore getFeatureStoreFromSolrConfigOrResources(
      final String featureStoreName, final SolrResourceLoader solrResourceLoader) {
    final FeatureStore featureStore = this.getFeatureStoreFromSolr(LOCAL_FEATURE_FOLDER,
        featureStoreName, solrResourceLoader);
    return featureStore;
  }
  
  /**
   * Loads a json feature store located in the solr config folder,
   *
   * @param featureStoreDirectory
   *          the resource/solrconfig directory containing the feature file (or
   *          null if the feature file is in the root folder)
   * @param featureStoreName
   *          name of the feature store
   * @param solrResourceLoader
   *          the solr resource loader
   * @return the feature store described in the json file, or an empty feature
   *         store in case of error
   */
  private FeatureStore getFeatureStoreFromSolr(
      final String featureStoreDirectory, final String featureStoreName,
      final SolrResourceLoader solrResourceLoader) {
    // check if the store is in the cache
    if (this.stores.containsKey(featureStoreName)) {
      return this.stores.get(featureStoreName);
    }
    FeatureStore featureStore = new FeatureStore(featureStoreName);
    
    final String featureStorePath = (featureStoreDirectory == null) ? featureStoreName
        : featureStoreDirectory + "/" + featureStoreName;
    try (InputStream is = solrResourceLoader.openResource(featureStorePath
        + ".json")) {
      try (Reader featureStoreReader = new InputStreamReader(is)) {
        featureStore = this.loadStore(featureStoreName, featureStoreReader);
      }
      // add the store in the cache
      this.stores.put(featureStoreName, featureStore);

    } catch (final Exception e) {
      logger.error("loading feature store in {}: \n{} ",featureStorePath, e);
    }
    // in case of error feature store will be empty
    return featureStore;
    
  }
  
}
