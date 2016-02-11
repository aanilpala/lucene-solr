package ltr.feature.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;

import ltr.feature.norm.Normalizer;
import ltr.ranking.Feature;
import ltr.util.CommonLtrParams;
import ltr.util.FeatureException;
import ltr.util.NamedParams;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This feature will return the number of terms in a query referring to a
 * particular type. For example the query:
 *
 * topic.US topic.UK person.OBAMA
 *
 * has two terms referring to the type 'topic', and one referring to the type
 * person. This is done by specifying the 'querytype' in the feature parameters
 * (eg. "querytype":"topic"), the feature will extract the terms from the query
 * and check if they start with the querytype. It will return the number of
 * distinct terms for a querytype or -1 in case of errors.
 */
public class QueryTypeFeature extends Feature {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles
      .lookup().lookupClass());

  private String querytype;

  public QueryTypeFeature() {

  }

  @Override
  public void init(final String name, final NamedParams params, final int id,
      final float defaultValue) throws FeatureException {
    super.init(name, params, id, defaultValue);
    if (!params.containsKey(CommonLtrParams.QUERY_TYPE)) {
      throw new FeatureException("missing param " + CommonLtrParams.QUERY_TYPE);
    }
  }

  @Override
  public FeatureWeight createWeight(final IndexSearcher searcher)
      throws IOException {
    this.querytype = (String) this.params.get(CommonLtrParams.QUERY_TYPE);
    return new QueryTypeWeight(searcher, this.name, this.params, this.norm,
        this.id);
  }

  @Override
  public String toString() {
    return "QueryTypeFeature [querytype:" + this.querytype + "]";
  }

  public class QueryTypeWeight extends FeatureWeight {

    private final Set<Term> terms = new HashSet<>();
    private float queryTypeFreq = 0;

    public QueryTypeWeight(final IndexSearcher searcher, final String name,
        final NamedParams params, final Normalizer norm, final int id) {
      super(searcher, name, params, norm, id);
    }

    @Override
    public Query getQuery() {
      return QueryTypeFeature.this;
    }

    @Override
    public void setOriginalQuery(final Query originalQuery) {
      if (originalQuery != null) {
        originalQuery.extractTerms(this.terms);
        for (final Term term : this.terms) {
          final String text = term.text();
          if ((text != null) && text.startsWith(QueryTypeFeature.this.querytype)) {
            this.queryTypeFreq++;
          }
        }
      } else {
        this.queryTypeFreq = QueryTypeFeature.this.defaultValue;
      }
    };

    @Override
    public FeatureScorer scorer(final AtomicReaderContext context,
        final Bits acceptDocs) throws IOException {
      return new QueryTypeScorer(this);
    }

    public class QueryTypeScorer extends FeatureScorer {

      public QueryTypeScorer(final FeatureWeight weight) {
        super(weight);
      }

      @Override
      public float score() {
        return QueryTypeWeight.this.queryTypeFreq;
      }

      @Override
      public String toString() {
        return "QueryTypeScorer [name=" + this.name + " querytype="
            + QueryTypeFeature.this.querytype + " ]";
      }

    }
  }

}
