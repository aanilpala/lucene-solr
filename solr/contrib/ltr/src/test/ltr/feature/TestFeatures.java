package ltr.feature;

import ltr.TestRerankBase;

import org.apache.solr.client.solrj.SolrQuery;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFeatures extends TestRerankBase {
  
  @BeforeClass
  public static void setup() throws Exception {
    setuptest();
    System.out.println("-----------index --------------------");
    // clean the current index before adding the documents
    assertU(delQ("*:*"));
    assertU(adoc("title", "bloomberg different bla", "description",
        "bloomberg", "id", "6", "popularity", "1", "content","blaTOP1 feature-BLA-test"));
    assertU(adoc("title", "bloomberg bloomberg ", "description",
        "bloomberg", "id", "7", "popularity", "2", "content", "blaTOP2"));
    assertU(adoc("title", "bloomberg bloomberg bloomberg", "description",
        "bloomberg", "id", "8", "popularity", "3"));
    assertU(adoc("title", "bloomberg bloomberg bloomberg bloomberg", "id",
        "9", "popularity", "5"));
    assertU(commit());
  }
  
  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }
  
  private SolrQuery getQuery(final String query, final String featureModel, final int rows) {
    final SolrQuery q = new SolrQuery();
    q.setQuery(query);
    q.add("rows", String.valueOf(rows));
    // get the feature vector binding it to the field name fv
    q.add("fl", "*,score,fv:[fv model=" + featureModel + "]");
    q.add("wt", "json");
    return q;
  }
  
  @Test
  public void testQueryTypeFeature() throws Exception {
    final SolrQuery query = this.getQuery("TOPIC.test TOPIC.test1 OR *:*",
        "test_query_type_feature", 1);
    
    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_query_type_feature:1.0;query_has_topic:2.0'");
  }
  
  @Test
  public void testQueryScore() throws Exception {
    final SolrQuery query = this.getQuery("title:bloomberg",
        "test_original_score_feature", 4);
    
    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_original_score_feature:1.0;original_score:0.7768564'");
    assertJQ("/query?" + query.toString(),
        "/response/docs/[3]/fv=='@test_original_score_feature:1.0;original_score:0.3884282'");
  }
  
  @Test
  public void testQueryFeatures() throws Exception {
    final SolrQuery query = this.getQuery("title:bloomberg", "query_features", 4);
    
    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@query_features:1.0;query_has_topic:0.0;original_score:0.7768564'");
  }
  
  @Test
  public void testFieldLengthFeature() throws Exception {
    final SolrQuery query = this.getQuery("*:*", "test_field_length_feature", 4);
    // please note that title_length here should be 3.0, while in test we check
    // for 4.0. The reason is that solr stores the lengths of the fields in a
    // compressed lossy format (1 byte per number), so sometimes the returned
    // value could be slightly different.
    //
    // see also: https://lucene.apache.org/core/5_2_0/core/org/apache/lucene/codecs/lucene50/Lucene50NormsFormat.html
    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_field_length_feature:1.0;title_length:4.0;description_length:1.0'");
  }

  @Test
  public void testFieldLengthFeatureWhereAFieldIsMissing() throws Exception {
    final SolrQuery query = this.getQuery("id:8 OR id:9", "test_content_field_length_feature", 4);

    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_content_field_length_feature:1.0;content_length:0.0'");
    assertJQ("/query?" + query.toString(),
        "/response/docs/[1]/fv=='@test_content_field_length_feature:1.0;content_length:0.0'");
  }



  
  @Test
  public void testFieldCharLengthFeature() throws Exception {
    final SolrQuery query = this.getQuery("title:\"bloomberg different bla\"",
        "test_field_char_length_feature", 4);
    
    final float titleCharLength = "bloomberg different bla".length();
    final float descriptionCharLength = "bloomberg".length();
    
    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_field_char_length_feature:1.0;title_char_length:" + titleCharLength
        + ";description_char_length:" + descriptionCharLength
        + "'");
  }

  @Test
  public void testFieldCharLengthFeatureWhereAFieldIsMissing() throws Exception {
    final SolrQuery query = this.getQuery("id:8 OR id:9", "test_content_field_char_length_feature", 4);

    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_content_field_char_length_feature:1.0;content_char_length:0.0'");
    assertJQ("/query?" + query.toString(),
        "/response/docs/[1]/fv=='@test_content_field_char_length_feature:1.0;content_char_length:0.0'");
  }
  
  @Test
  public void testFunctionQueryFeature() throws Exception {
    final SolrQuery query = this.getQuery("popularity:3",
        "test_function_query_feature", 4);
    
    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_function_query_feature:1.0;powpolarity:9.0'");
    
    query.setQuery("popularity:5");
    
    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_function_query_feature:1.0;powpolarity:25.0'");
  }
  
  @Test
  public void testConstantFeature() throws Exception {
    final SolrQuery query = this.getQuery("*:*", "test_constant_feature", 1);
    
    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_constant_feature:1.0;c1:42.0'");
  }
  
  @Test
  public void testCompanionCodeFeatureIfQueryIsNotATerm() throws Exception {
    final float defaultValueCompanion = 0.0f;
    final float defaultValueCompanion2 = -1.0f;

    final SolrQuery query = this.getQuery("*:*", "test_companioncode_feature", 1);


    // if the query is not not a term query, the feature return a default value
    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_companioncode_feature:1.0;companion:"+defaultValueCompanion+";companion2:"+defaultValueCompanion2+"'");

    query.setQuery("title:bla title:bloomberg");
    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_companioncode_feature:1.0;companion:"+defaultValueCompanion+";companion2:"+defaultValueCompanion2+"'");

  }

  @Test
  public void testCompanionCodeFeature() throws Exception {

    final SolrQuery query = this.getQuery("title:bla", "test_companioncode_feature", 1);

    // if the query is not not a term query, the feature return a default value
    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_companioncode_feature:1.0;companion:"+3.0+";companion2:"+42.0+"'");
  }

  @Test
  public void testCompanionCodeFeatureTestNoMatch() throws Exception {
    //test a matching companion code on a document that does not match
    final float defaultValueCompanion = 0.0f;
    final float defaultValueCompanion2 = -1.0f;
    final SolrQuery query = this.getQuery("id:8", "test_companioncode_feature", 4);

    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_companioncode_feature:1.0;companion:"+defaultValueCompanion+";companion2:"+defaultValueCompanion2+"'");
  }

  @Test
  public void testCompanionCodeFeatureTestNoMatch2() throws Exception {
    // test a companion code on a document that matches the query but not
    // the companion codes
    final SolrQuery query = this.getQuery("title:bla", "test_companioncode_not_matching", 4);

    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_companioncode_not_matching:1.0;companion:"+0f+"'");
  }

  @Test
  public void testCompanionCodeFeatureTestNoMatchSuppliedDefault() throws Exception {
    // test a companion code on a document that matches the query but not
    // the companion codes
    final SolrQuery query = this.getQuery("title:bla", "test_companioncode_not_matching_default_set_to_42", 4);

    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_companioncode_not_matching_default_set_to_42:1.0;companion:"+42f+"'");
  }

  @Test
  public void testCompanionCodeFeatureMultipleMatches() throws Exception {
    // test multiple matches
    final SolrQuery query = this.getQuery("title:bla", "test_companioncode_feature_multiple_matches", 1);

    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_companioncode_feature_multiple_matches:1.0;companion:111.0'");
  }

  // used to check the output of assertJQ
  private void print(final String query, final String nil) throws Exception{
    System.out.println("query: "+query);
    System.out.println(restTestHarness.query(query));
  }


  
  @Test
  public void testHasFieldFeature() throws Exception {
    // this document doesn't have the field description
    final SolrQuery query = this.getQuery("id:9", "test_has_field_feature", 4);
    
    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_has_field_feature:1.0;has_title:1.0;has_description:0.0'");
    
    query.setQuery("popularity:1");
    assertJQ("/query?" + query.toString(),
        "/response/docs/[0]/fv=='@test_has_field_feature:1.0;has_title:1.0;has_description:1.0'");
  }
  
}
