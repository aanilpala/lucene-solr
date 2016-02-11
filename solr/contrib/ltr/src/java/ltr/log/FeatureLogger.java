package ltr.log;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FeatureLogger can be registered in a model and provide a strategy for logging
 * the feature values.
 */
public abstract class FeatureLogger<RECORD> {

  // contains the mapping docid -> computed features
  private final Map<Integer,RECORD> docIdToFeatures;

  public enum Format {
    CSV, JSON
  }

  private static final Logger logger = LoggerFactory
      .getLogger(MethodHandles
          .lookup().lookupClass());

  public FeatureLogger(final int numDocsToRerank) {
    this.docIdToFeatures = new HashMap<>(numDocsToRerank);
  }

  /**
   * returns a FeatureLogger that logs the features in output, using the format
   * specified in the 'format' param: - csv will log the features as a unique
   * string in csv format - json will log the features in a map in a
   * Map<feature_name,featurevalue> if format is null or empty, csv format will
   * be selected.
   *
   * @param format
   *          the format to use
   * @param numDocsToRerank
   *          how many documents are reranked
   * @return a feature logger for the format specified.
   */
  public static FeatureLogger<?> getFeatureLogger(final Format format,
      int numDocsToRerank) {
    if (numDocsToRerank < 0) {
      logger.warn("invalid number of documents to rerank ({}), set it to 0",
          numDocsToRerank);
      numDocsToRerank = 0;
    }
    FeatureLogger<?> featureLogger;
    switch (format) {
      case CSV:
        featureLogger = new CSVFeatureLogger(numDocsToRerank);
        break;
      case JSON:
        featureLogger = new MapFeatureLogger(numDocsToRerank);
        break;
      default:
        logger.warn("unknown feature logger {}", format);
        featureLogger = new CSVFeatureLogger(numDocsToRerank);
        break;
    }
    return featureLogger;
  }

  /**
   * Log will be called every time that the model generates the feature values
   * for a document and a query.
   *
   * @param docid
   *          Solr document id whose features we are saving
   * @param featureStoreName
   *          name of current feature store
   * @param featureStoreVersion
   *          version of the current feature store
   * @param featureNames
   *          List of all the feature names we are logging
   * @param featureValues
   *          Parallel list to featureNames that stores all the unnormalized
   *          feature values
   *
   * @return true if the logger successfully logged the features, false
   *         otherwise.
   */

  public boolean log(final int docid, final String featureStoreName,
      final float featureStoreVersion, final String[] featureNames,
      final float[] featureValues) {
    final RECORD r = this.makeRecord(docid, featureStoreName,
        featureStoreVersion, featureNames, featureValues);
    if (r == null) {
      return false;
    }
    this.docIdToFeatures.put(docid, r);
    return true;
  }

  /**
   * populate the document with its feature vector
   *
   * @param docid
   *          Solr document id
   * @return String representation of the list of features calculated for docid
   */
  public RECORD getFeatureVector(final int docid) {
    return this.docIdToFeatures.get(docid);
  }

  /**
   * given a docid, a feature store name, its version, a list of feature names,
   * and a list of feature values, so that the string at index i in the feature
   * name list is the name of the feature value at index i in the feature
   * values, it will generate a record object of type RECORD containing the
   * values.
   *
   * @param docid
   *          the docid of the document for which the features are produced
   * @param featureStoreName
   *          the name of the feature store used to compute the features
   * @param featureStoreVersion
   *          the version of the feature store used to compute the features
   * @param featureNames
   *          a list containing all the names features computed
   * @param featureValues
   *          a list containing all the values of the features computed (in the
   *          same order of the names)
   * @return a object of type RECORD containing all the values to log.
   */
  public abstract RECORD makeRecord(int docid, String featureStoreName,
      float featureStoreVersion, String[] featureNames, float[] featureValues);

  /**
   * This class will produce a Map containing the feature values to serialize,
   * it will be used to encode the feature in the response as a map (in json/xml
   * etc).
   */
  public static class MapFeatureLogger extends FeatureLogger<Map<String,Float>> {

    public MapFeatureLogger(final int numDocsToRerank) {
      super(numDocsToRerank);
    }

    @Override
    public Map<String,Float> makeRecord(final int docid,
        final String featureStoreName, final float featureStoreVersion,
        final String[] featureNames, final float[] featureValues) {
      final Map<String,Float> featureMap = new HashMap<>(featureValues.length);
      featureMap.put("@" + featureStoreName, featureStoreVersion);
      for (int i = 0; i < featureNames.length; i++) {
        featureMap.put(featureNames[i], featureValues[i]);
      }
      return featureMap;
    }
  }

  /**
   * This class will produce a string containing all the feature values to
   * serialize, encoded using the CSV format.
   */
  public static class CSVFeatureLogger extends FeatureLogger<String> {
    StringBuilder sb = new StringBuilder(500);
    char keyValueSep = ':';
    char featureSep = ';';

    public CSVFeatureLogger(final int numDocsToRerank) {
      super(numDocsToRerank);
    }

    public CSVFeatureLogger setKeyValueSep(final char keyValueSep) {
      this.keyValueSep = keyValueSep;
      return this;
    }

    public CSVFeatureLogger setFeatureSep(final char featureSep) {
      this.featureSep = featureSep;
      return this;
    }

    @Override
    public String makeRecord(final int docid, final String featureStoreName,
        final float featureStoreVersion, final String[] featureNames,
        final float[] featureValues) {
      this.sb.append('@').append(featureStoreName).append(this.keyValueSep);
      this.sb.append(featureStoreVersion).append(this.featureSep);
      for (int i = 0; i < featureNames.length; i++) {
        this.sb.append(featureNames[i]).append(this.keyValueSep)
        .append(featureValues[i]);
        this.sb.append(this.featureSep);
      }

      final String features = (this.sb.length() == 0) ? "" : this.sb.substring(
          0, this.sb.length() - 1);
      this.sb.setLength(0);
      return features;
    }

  }

}
