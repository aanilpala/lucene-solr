package ltr.feature;

import java.util.Collection;
import java.util.LinkedHashMap;

import ltr.ranking.Feature;
import ltr.util.LtrException;

/**
 * A feature store will maintain the list of couples <feature name, feature
 * instance> Each feature name has a unique name.
 *
 */
public class FeatureStore {
  // uses a LinkedHashMap to maintain feature-name -> Feature
  // and remember the order of insertion of the features.
  private final LinkedHashMap<String,Feature> store = new LinkedHashMap<>();
  private final String storeName;
  public static final float NO_VERSION = -1;
  // current version for this feature store (null means no version declared)
  private float version = NO_VERSION;

  /**
   * @return the storeName
   */
  public String getStoreName() {
    return this.storeName;
  }

  public boolean isEmpty() {
    return this.store.isEmpty();
  }

  /**
   * Create a new feature store with the given name
   *
   * @param storeName
   *          the name of this feature store
   */
  public FeatureStore(final String storeName) {
    this.storeName = storeName;
  }


  /**
   * @return the size of this feature store
   */
  public int size() {
    return this.store.size();
  }

  /**
   * @param name
   *          the name of a feature
   * @return true if the feature is contained in this store
   */
  public boolean containsFeature(final String name) {
    return this.store.containsKey(name);
  }

  /**
   * add the a feature to this store
   *
   * @param feature
   *          the feature to add
   */
  public void add(final Feature feature) {
    this.store.put(feature.getName(), feature);
  }

  /**
   * @return all the features in this store, sorted by insertion order
   */
  public Collection<Feature> getFeatures() {
    return this.store.values();
  }

  /**
   * removes all the features registered in this feature store
   */
  public void clear() {
    this.store.clear();

  }

  /**
   * set the current version of this feature store
   *
   * @param version
   *          - the version of the feature store
   * @throws LtrException
   *          - if version number is lower than zero
   */
  public void setVersion(final float version) throws LtrException {
    if (version < 0){
      throw new LtrException("invalid version should be a positive number");
    }
    this.version = version;
  }

  /**
   * returns the current version for this feature store
   *
   * @return the version for this feature store
   */
  public float getVersion() {
    return this.version;
  }

}
