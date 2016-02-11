package ltr.feature.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
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
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This feature will check for the presence of one or more companion codes in
 * case the query is single term. See [NSREX-11]. This feature will accept two
 * params: 1. code_values 2. field
 *
 * e.g. "params":
 *      { "code_values" :
 *        { "${q}TEST": 1.0,
 *          "feature-${q}-TEST": 42.0,
 *          "${q}TOP": 2.0,
 *          "${q}TOP1": 3.0 },
 *        "field":"content",
 *       },
 *
 * code_values will contain the companion codes and the feature value mapped to
 * each companion code. ${q} will be replaced with the current query, but it's
 * not mandatory to specify a query, a companion code can be also a simple
 * string.
 *
 *
 * Each companion code will be matched against the field specified (content, in
 * the example) and if the field contains a string that matches the companion
 * code, the assigned feature value for the companion code will be returned. The
 * matching follows the declaration order so in case of multiple matches, the
 * value of the feature will be the value associated with the first companion
 * code that matched within the field.
 *
 * If the query is not a simple term, or if no companion code was matched in the
 * document, this feature will return the value the default value set for the feature
 *
 * Example: the feature in the example applied on
 * "content: topic.OILTOP1 topic.OILTEST  " will produce the value 1.0 ,since
 * ${q}TEST will be the first companion code matched on the field.
 *
 *
 * TODO: we could think of having this value returning the average of the scores
 * when applied to multi-term queries (up to a certain number of topics AND
 * using only the "positive" ones -- i.e., not the ones like "NOT TOPIC:OIL").
 *
 */
public class CompanionCodeFeature extends Feature {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles
      .lookup().lookupClass());

  private static final String CODE_VALUES_PARAM = "code_values";

  // the default field where to look for the companion codes
  private static final String DEFAULT_FIELD = "content";

  private String[] companionCodes;
  private float[] companionCodeFeatureValue;
  private String companionCodeField;

  public CompanionCodeFeature() {}

  @Override
  @SuppressWarnings("unchecked")
  public void init(final String name, final NamedParams params, final int id,
      final float defaultValue) throws FeatureException {
    super.init(name, params, id, defaultValue);
    this.companionCodes = new String[0];
    this.companionCodeFeatureValue = new float[0];
    Map<String,Double> companionToFeatureScore;

    if (params.containsKey(CODE_VALUES_PARAM)) {
      companionToFeatureScore = (Map<String,Double>) params
          .get(CODE_VALUES_PARAM);
    } else {
      logger.error("parsing companion code feature {}, cannot find {}", name,
          CODE_VALUES_PARAM);
      throw new FeatureException("parsing companion code feature");
    }

    this.companionCodeField = params.getString(CommonLtrParams.FIELD,
        DEFAULT_FIELD);

    this.companionCodes = new String[companionToFeatureScore.size()];
    this.companionCodeFeatureValue = new float[companionToFeatureScore.size()];
    int index = 0;
    // reads the mapping companion code -> feature value
    for (final Map.Entry<String,Double> companionCodeAndFeatureValue : companionToFeatureScore
        .entrySet()) {

      this.companionCodes[index] = companionCodeAndFeatureValue.getKey();
      // real are parsed as Double, so we need to convert them to floats
      // here.
      this.companionCodeFeatureValue[index++] = companionCodeAndFeatureValue
          .getValue().floatValue();
    }
  }

  @Override
  public FeatureWeight createWeight(final IndexSearcher searcher)
      throws IOException {
    return new CompanionCodeWeight(searcher, this.name, this.params, this.norm,
        this.id);
  }

  @Override
  public String toString() {
    return "CompanionCodeFeature";
  }

  public class CompanionCodeWeight extends FeatureWeight {

    Set<Term> terms = new HashSet<>();
    // in order to match if a document contains the specified companion
    // code, we extract the posting list for the term representing the companion
    // code, and we check if the docid is in the posting list.
    // This array of weights will contain the weight of each TermQuery created
    // for each companion code.
    Weight[] weights = new Weight[CompanionCodeFeature.this.companionCodeFeatureValue.length];

    public CompanionCodeWeight(final IndexSearcher searcher, final String name,
        final NamedParams params, final Normalizer norm, final int id) {
      super(searcher, name, params, norm, id);
    }

    @Override
    public Query getQuery() {
      return CompanionCodeFeature.this;
    }

    @Override
    public void setOriginalQuery(final Query originalQuery) throws IOException {
      if (originalQuery == null) {
        // if the query is not present ignore
        logger.debug("original query is null");
        return;
      }
      // if the query is not a single term query ignore
      originalQuery.extractTerms(this.terms);
      if (this.terms.size() != 1) {
        logger.debug("original query contains more than one term ({})",
            this.terms);
        return;
      }
      // the query contains exactly one term
      final Term term = this.terms.iterator().next();
      // Set the original query, since this method is called to prepare
      // the feature computation for all the documents, it will also
      // take care to produce the weights for all the companion code
      // queries.
      // TODO add a prepare() method called before the feature values generation
      for (int index = 0; index < CompanionCodeFeature.this.companionCodes.length; index++) {

        Query tq = null;
        final String termText = term.text();
        // We need to extract the posting list for each companion code,
        // so we build a query for each companion code. If the companion
        // code mentions the query, we replace the query before building
        // the query.
        final String query = CompanionCodeFeature.this.companionCodeField
            + ":"
            + CompanionCodeFeature.this.companionCodes[index].replace("${q}",
                termText);
        try {

          final QParser parser = QParser.getParser(query, "lucene",
              this.request);
          tq = parser.parse();
        } catch (final SyntaxError e) {
          logger.warn("error parsing companion code query {} ", query);
          continue;
        }
        this.weights[index] = tq.createWeight(this.searcher);
        this.weights[index].getValueForNormalization();
      }

    }

    @Override
    public FeatureScorer scorer(final AtomicReaderContext context,
        final Bits acceptDocs) throws IOException {

      final Scorer[] companionCodeScorers = new Scorer[this.weights.length];
      for (int index = 0; index < this.weights.length; index++) {
        if (this.weights[index] != null) {
          final Scorer scorer = this.weights[index].scorer(context,
              Weight.PostingFeatures.DOCS_AND_FREQS, acceptDocs);
          companionCodeScorers[index] = scorer;
        }
      }
      return new CompanionCodeScorer(this, context, companionCodeScorers);
    }

    /*
     * This scorer will wrap all the scores produced for the companion code term
     * queries, and will return as score the value associated with the first
     * companion code matching in the list for the current document.
     */
    public class CompanionCodeScorer extends FeatureScorer {

      private Scorer[] companionCodeScorers = null;

      public CompanionCodeScorer(final FeatureWeight weight,
          final AtomicReaderContext context, final Scorer[] companionCodeScorers) {
        super(weight);
        this.companionCodeScorers = companionCodeScorers;
      }

      @Override
      public float score() throws IOException {
        if (CompanionCodeWeight.this.terms.size() != 1) {
          throw new FeatureException(
              "companion code can be computed only for single term queries");
        }
        for (int index = 0; index < this.companionCodeScorers.length; index++) {
          if (this.companionCodeScorers[index] == null) {
            continue;
          }
          // iterates on the posting list of this companion code. If we
          // found the docId, it means that the companion code was
          // the given document, and we can return the feature value
          // associated with the companion code.
          if (this.companionCodeScorers[index].advance(this.docID) == this.docID) {
            return CompanionCodeFeature.this.companionCodeFeatureValue[index];
          }
        }
        return CompanionCodeFeature.this.defaultValue;
      }

      @Override
      public String toString() {
        return "CompanionCodeFeature ["
            + Arrays.toString(CompanionCodeFeature.this.companionCodes) + "]";
      }

    }
  }

}
