package ltr.ranking;

import java.io.IOException;
import java.util.Map;

import ltr.feature.norm.Normalizer;
import ltr.feature.norm.impl.IdentityNormalizer;
import ltr.util.FeatureException;
import ltr.util.NamedParams;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.util.Bits;
import org.apache.solr.request.SolrQueryRequest;

/**
 * A 'recipe' for computing a feature.
 */
public abstract class Feature extends Query {
  // A feature will produce a score for each document given a query so
  // it is considered a scoring function, and extends the lucene Query object.
  // This will allow
  // as to reuse lucene/solr classes in order to compute particular features.
  
  protected String name;
  protected String type = this.getClass().getCanonicalName();
  protected Normalizer norm = IdentityNormalizer.INSTANCE;
  public int id;
  protected NamedParams params = NamedParams.EMPTY;
  protected float defaultValue;

  public Feature() {
    
  }
  
  /**
   * initialize a feature
   *
   * @param name
   *          the symbolic name of the feature.
   * @param params
   *          optional params used to compute the feature
   * @param id
   *          a unique integer id associated to the feature
   * @param defaultValue
   *          the defaultValue for this feature, if an error occurs during this
   *          computation of the feature, this value will be assigned
   * @throws FeatureException
   *           if something fails during the initialization
   */
  public void init(final String name, final NamedParams params, final int id,
      final float defaultValue) throws FeatureException {
    this.name = name;
    this.params = params;
    this.id = id;
    this.defaultValue = defaultValue;
  }
  

  
  @Override
  public String toString(final String field) {
    return this.toString();
  }
  
  @Override
  public String toString() {
    return "Feature [name=" + this.name + ", type=" + this.type + ", norm="
        + this.norm + ", id=" + this.id + ", params=" + this.params
        + ", defaultValue=" + this.defaultValue + "]";
  }
  
  @Override
  public abstract FeatureWeight createWeight(IndexSearcher searcher)
      throws IOException;
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = (prime * result) + Float.floatToIntBits(this.defaultValue);
    result = (prime * result) + this.id;
    result = (prime * result)
        + ((this.name == null) ? 0 : this.name.hashCode());
    result = (prime * result)
        + ((this.norm == null) ? 0 : this.norm.hashCode());
    result = (prime * result)
        + ((this.params == null) ? 0 : this.params.hashCode());
    result = (prime * result)
        + ((this.type == null) ? 0 : this.type.hashCode());
    return result;
  }
  
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (this.getClass() != obj.getClass()) {
      return false;
    }
    final Feature other = (Feature) obj;
    if (Float.floatToIntBits(this.defaultValue) != Float
        .floatToIntBits(other.defaultValue)) {
      return false;
    }
    if (this.id != other.id) {
      return false;
    }
    if (this.name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!this.name.equals(other.name)) {
      return false;
    }
    if (this.norm == null) {
      if (other.norm != null) {
        return false;
      }
    } else if (!this.norm.equals(other.norm)) {
      return false;
    }
    if (this.params == null) {
      if (other.params != null) {
        return false;
      }
    } else if (!this.params.equals(other.params)) {
      return false;
    }
    if (this.type == null) {
      if (other.type != null) {
        return false;
      }
    } else if (!this.type.equals(other.type)) {
      return false;
    }
    return true;
  }
  
  /**
   * @return the name
   */
  public String getName() {
    return this.name;
  }
  
  /**
   * @return the norm
   */
  public Normalizer getNorm() {
    return this.norm;
  }
  
  /**
   * @return the id
   */
  public int getId() {
    return this.id;
  }
  
  /**
   * @return the params
   */
  public NamedParams getParams() {
    return this.params;
  }
  
  /**
   * return the default value for a feature, in case of error this value would
   * be assigned to the feature.
   *
   * @return the default value for this feature
   */
  public float getDefaultValue() {
    return this.defaultValue;
  }
  
  /**
   * set a normalizer for this feature, if a normalizer is registered the value
   * of the features will be normalized.
   *
   * @param norm
   *          the normalizer for this feature
   */
  public void setNorm(final Normalizer norm) {
    this.norm = norm;
    
  }
  
  public abstract class FeatureWeight extends Weight implements Cloneable {
    
    protected String name;
    protected NamedParams params = NamedParams.EMPTY;
    protected Normalizer norm = IdentityNormalizer.INSTANCE;
    protected IndexSearcher searcher;
    protected SolrQueryRequest request;
    protected Map<String,String> efi;
    protected Query originalQuery;
    public int id;
    
    /**
     * Initialize a feature without the normalizer from the feature file. This
     * is called on initial construction since multiple models share the same
     * features, but have different normalizers. A concrete model's feature is
     * copied through featForNewModel().
     *
     * @param searcher
     *          Solr searcher available for features if they need them
     *
     * @param name
     *          Name of the feature
     * @param params
     *          Custom parameters that the feature may use
     * @param norm
     *          Feature normalizer used to normalize the feature value
     * @param id
     *          Unique ID for this feature. Similar to feature name, except it
     *          can be used to directly access the feature in the global list of
     *          features.
     */
    public FeatureWeight(final IndexSearcher searcher, final String name,
        final NamedParams params, final Normalizer norm, final int id) {
      this.searcher = searcher;
      this.name = name;
      this.params = params;
      this.id = id;
      this.norm = norm;
    }
    
    public final void setRequest(final SolrQueryRequest request) {
      this.request = request;
    }
    
    public final void setExternalFeatureInfo(final Map<String,String> efi) {
      this.efi = efi;
    }
    
    public float getDefaultValue() {
      return Feature.this.defaultValue;
    }
    
    /**
     * Called once after all parameters have been set on the weight. Override
     * this to do things with the original query, request, or external
     * parameters.
     *
     * @throws IOException
     *           in case of errors accessing the index
     */
    public void process() throws IOException {}
    
    public String getName() {
      return this.name;
    }
    
    public Normalizer getNorm() {
      return this.norm;
    }
    
    public NamedParams getParams() {
      return this.params;
    }
    
    public int getId() {
      return this.id;
    }
    
    @Override
    public Scorer scorer(final AtomicReaderContext context,
        final PostingFeatures arg1, final Bits acceptDocs) throws IOException {
      return this.scorer(context, acceptDocs);
    }
    
    public abstract FeatureScorer scorer(AtomicReaderContext context,
        Bits acceptDocs) throws IOException;
    
    @Override
    public Explanation explain(final AtomicReaderContext context, final int doc)
        throws IOException {
      final FeatureScorer r = this.scorer(context, null);
      r.advance(doc);
      float score = Feature.this.defaultValue;
      if (r.docID() == doc) {
        score = r.score();
      }
      final Explanation e = new Explanation(score, r.toString());
      return e;
    }
    
    @Override
    public float getValueForNormalization() throws IOException {
      return 1f;
    }
    
    @Override
    public void normalize(final float norm, final float topLevelBoost) {
      // For advanced features that use Solr weights internally, you must
      // override
      // and pass this call on to them
    }
    
    @Override
    public String toString() {
      return this.getClass().getName() + " [name=" + this.name + ", params="
          + this.params + "]";
    }
    
    /**
     * @param originalQuery
     *          the originalQuery to set
     * @throws IOException
     *           this method could be used to manipulate the query and could
     *           throw IOExceptions
     */
    public void setOriginalQuery(final Query originalQuery) throws IOException {
      this.originalQuery = originalQuery;
    }
    
    /**
     * A 'recipe' for computing a feature
     *
     * @see Feature
     */
    public abstract class FeatureScorer extends Scorer {
      
      protected int docID = -1;
      protected String name;
      
      public FeatureScorer(final FeatureWeight weight) {
        super(weight);
        this.name = weight.getName();
      }
      
      @Override
      public abstract float score() throws IOException;
      
      /**
       * Used in the FeatureWeight's explain. Each feature should implement this
       * returning properties of the specific scorer useful for an explain. For
       * example "MyCustomClassFeature [name=" + name + "myVariable:" +
       * myVariable + "]";
       */
      @Override
      public abstract String toString();
      
      @Override
      public int freq() throws IOException {
        throw new UnsupportedOperationException();
      }
      
      @Override
      public int docID() {
        return this.docID;
      }
      
      @Override
      public int nextDoc() throws IOException {
        // only the rescorer will call this scorers and nextDoc will never be
        // called
        throw new UnsupportedOperationException();
      }
      
      @Override
      public int advance(final int target) throws IOException {
        // For advanced features that use Solr scorers internally,
        // you must override and pass this call on to them
        this.docID = target;
        return this.docID;
      }
      
      @Override
      public long cost() {
        throw new UnsupportedOperationException();
      }
      
      public float getDefaultValue() {
        return Feature.this.defaultValue;
      }
      
      @Override
      public IntervalIterator intervals(final boolean arg0) throws IOException {
        throw new UnsupportedOperationException();
      }
      

      
    }

    /**
     * Default FeatureScorer class that returns the score passed in. Can be
     * used as a simple ConstantFeature, or to return a default scorer in case
     * an underlying feature's scorer is null.
     */
    public class ConstantFeatureScorer extends FeatureScorer {
      
      float constScore;
      String featureType;
      
      public ConstantFeatureScorer(final FeatureWeight weight,
          final float constScore, final String featureType) {
        super(weight);
        this.constScore = constScore;
        this.featureType = featureType;
      }
      
      @Override
      public float score() {
        return this.constScore;
      }
      
      @Override
      public String toString() {
        return this.featureType + " [name=" + this.name + " value="
            + this.constScore + "]";
      }
      
    }

    

  }
}