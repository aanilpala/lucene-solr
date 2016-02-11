package ltr.feature.norm;

import ltr.feature.norm.impl.IdentityNormalizer;
import ltr.feature.norm.impl.StandardNormalizer;
import ltr.util.NamedParams;
import ltr.util.NormalizerException;

import org.apache.lucene.search.Explanation;

/**
 * A normalizer normalizes the value of a feature. Once that the feature values
 * will be computed, the normalizer will be applied and the resulting values
 * will be received by the model.
 *
 * @see IdentityNormalizer
 * @see StandardNormalizer
 *
 */
public abstract class Normalizer {
  
  protected String type = this.getClass().getCanonicalName();
  private NamedParams params;
  
  public String getType() {
    return this.type;
  }
  
  public NamedParams getParams() {
    return this.params;
  }
  
  public void setType(final String type) {
    this.type = type;
  }
  
  public void init(final NamedParams params) throws NormalizerException {
    this.params = params;
  }
  
  public abstract float normalize(float value);
  
  public Explanation explain(final Explanation explain) {
    final float normalized = this.normalize(explain.getValue());
    String explainDesc = "normalized using " + this.type;
    if (this.params != null) {
      explainDesc += " [params " + this.params + "]";
    }
    final Explanation normExplain = new Explanation(normalized, explainDesc);
    normExplain.addDetail(explain);
    return normExplain;
  }
  
}
