package ltr.feature.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import ltr.feature.norm.Normalizer;
import ltr.ranking.Feature;
import ltr.util.CommonLtrParams;
import ltr.util.FeatureException;
import ltr.util.NamedParams;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This feature will return the 'age' of a story. Given a field contained a date
 * (could be the last-update, or the create date) the feature will contained the
 * time elapsed from the field date to the request date. You can get the delta
 * with different granularities (specified in the field 'granularity' (seconds,
 * minutes, hours, days)).
 */
public class StoryAgeFeature extends Feature {
  
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles
      .lookup().lookupClass());
  
  enum Granularity {
    seconds, minutes, hours, days
  }
  
  private String field;
  private final Set<String> fields = new HashSet<>();
  private Granularity granularity;
  
  public StoryAgeFeature() {
    
  }
  
  @Override
  public void init(final String name, final NamedParams params, final int id,
      final float defaultValue) throws FeatureException {
    super.init(name, params, id, defaultValue);
    if (!params.containsKey(CommonLtrParams.FIELD)) {
      throw new FeatureException("missing param " + CommonLtrParams.FIELD);
    }
    if (!params.containsKey("granularity")) {
      throw new FeatureException("missing param granularity");
    }
    try {
      final String strGranularity = (String) params.get("granularity");
      this.granularity = Granularity.valueOf(strGranularity);
      
    } catch (IllegalArgumentException | NullPointerException e) {
      logger
      .error("invalid or missing value for field granularity, accepted values are: seconds, minutes, hours, and days");
      throw new FeatureException(
          "invalid or missing value for field granularity, accepted values are: seconds, minutes, hours, and days",
          e);
    }
  }
  
  @Override
  public FeatureWeight createWeight(final IndexSearcher searcher)
      throws IOException {
    this.field = (String) this.params.get(CommonLtrParams.FIELD);
    this.fields.add(this.field);
    return new StoryAgeWeight(searcher, this.name, this.params, this.norm,
        this.id);
  }
  
  @Override
  public String toString() {
    return "DocValueFeature [field:" + this.field + "]";
    
  }
  
  public class StoryAgeWeight extends FeatureWeight {
    
    public StoryAgeWeight(final IndexSearcher searcher, final String name,
        final NamedParams params, final Normalizer norm, final int id) {
      super(searcher, name, params, norm, id);
    }
    
    @Override
    public Query getQuery() {
      return StoryAgeFeature.this;
    }
    
    @Override
    public FeatureScorer scorer(final AtomicReaderContext context,
        final Bits acceptDocs) throws IOException {
      return new StoryAgeScorer(this, context);
    }
    
    public class StoryAgeScorer extends FeatureScorer {
      
      private final AtomicReaderContext context;
      
      public StoryAgeScorer(final FeatureWeight weight,
          final AtomicReaderContext context) {
        super(weight);
        this.context = context;
      }
      
      @Override
      public float score() {
        try {
          final Document doc = this.context.reader().document(this.docID,
              StoryAgeFeature.this.fields);
          
          final IndexableField f = doc.getField(StoryAgeFeature.this.field);
          
          final long ldate = f.numericValue().longValue();
          
          final long current = StoryAgeWeight.this.request.getStartTime();
          final long delta = current - ldate;
          switch (StoryAgeFeature.this.granularity) {
            case seconds:
              return TimeUnit.MILLISECONDS.toSeconds(delta);
            case minutes:
              return TimeUnit.MILLISECONDS.toMinutes(delta);
            case hours:
              return TimeUnit.MILLISECONDS.toHours(delta);
            case days:
              return TimeUnit.MILLISECONDS.toDays(delta);
          }
          
        } catch (final IOException e) {
          logger.debug("error computing story age feature: {}", e);
        }
        return StoryAgeFeature.this.defaultValue;
      }
      
      @Override
      public String toString() {
        return "StoryAgeScorer [name=" + this.name + " fields="
            + StoryAgeFeature.this.fields + " granularity="
            + StoryAgeFeature.this.granularity + " ]";
      }
      
    }
  }
  
}
