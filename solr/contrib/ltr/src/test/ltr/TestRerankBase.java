package ltr;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import ltr.feature.impl.ConstantFeature;
import ltr.ranking.Feature;
import ltr.util.CommonLtrParams;
import ltr.util.FeatureException;
import ltr.util.NamedParams;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.util.RestTestBase;
import org.eclipse.jetty.servlet.ServletHolder;
import org.restlet.ext.servlet.ServerServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressSSL
public class TestRerankBase extends RestTestBase {
  
  private static final Logger logger = LoggerFactory.getLogger(TestRerankBase.class);
  
  protected static File tmpSolrHome;
  protected static File tmpConfDir;
  
  protected static final String collection = "collection1";
  protected static final String confDir = collection + "/conf";
  
  public static void setuptest() throws Exception {
    setuptest("solrconfig.xml", "schema.xml");
  }
  
  public static void setupPersistenttest() throws Exception {
    setupPersistentTest("solrconfig.xml", "schema.xml");
    bulkIndex();
  }
  
  public static void setuptest(final String solrconfig, final String schema) throws Exception {
    initCore(solrconfig, schema);
    
    tmpSolrHome = createTempDir();
    tmpConfDir = new File(tmpSolrHome, confDir);
    tmpConfDir.deleteOnExit();
    FileUtils.copyDirectory(new File(TEST_HOME()), tmpSolrHome.getAbsoluteFile());
    
    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), solrconfig, schema, "/solr", true, null);
  }
  
  public static void setupPersistentTest(final String solrconfig, final String schema) throws Exception {
    initCore(solrconfig, schema);
    
    tmpSolrHome = createTempDir();
    tmpConfDir = new File(tmpSolrHome, confDir);
    tmpConfDir.deleteOnExit();
    FileUtils.copyDirectory(new File(TEST_HOME()), tmpSolrHome.getAbsoluteFile());

    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<>();
    final ServletHolder solrRestApi = new ServletHolder("SolrSchemaRestApi", ServerServlet.class);
    solrRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SolrSchemaRestApi");
    solrRestApi.setInitParameter("storageIO", "org.apache.solr.rest.ManagedResourceStorage$JsonStorageIO");
    
    extraServlets.put(solrRestApi, "/config/*"); // '/schema/*' matches

    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "false");
    
    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), solrconfig, schema, "/solr", true, extraServlets);
  }
  
  protected static void aftertest() throws Exception {
    
    jetty.stop();
    jetty = null;
    FileUtils.deleteDirectory(tmpSolrHome);
    System.clearProperty("managed.schema.mutable");
    System.clearProperty("enable.update.log");
    
    restTestHarness = null;
  }
  
  public static void makeRestTestHarnessNull() {
    restTestHarness = null;
  }
  
  
  protected List<Feature> getFeatures(final List<String> names) throws FeatureException {
    final List<Feature> features = new ArrayList<>();
    int pos = 0;
    for (final String name : names) {
      final ConstantFeature f = new ConstantFeature();
      f.init(name, new NamedParams().add(CommonLtrParams.VALUE, 10), pos, CommonLtrParams.FEATURE_DEFAULT_VALUE);
      features.add(f);
      ++pos;
    }
    return features;
  }
  
  protected List<Feature> getFeatures(final String[] names) throws FeatureException {
    return this.getFeatures(Arrays.asList(names));
  }
  
  protected static void bulkIndex() throws Exception {
    System.out.println("-----------index ---------------------");
    assertU(adoc("title", "bloomberg different bla", "description", "bloomberg", "id", "6", "popularity", "1"));
    assertU(adoc("title", "bloomberg bloomberg ", "description", "bloomberg", "id", "7", "popularity", "2"));
    assertU(adoc("title", "bloomberg bloomberg bloomberg", "description", "bloomberg", "id", "8", "popularity", "3"));
    assertU(adoc("title", "bloomberg bloomberg bloomberg bloomberg", "description", "bloomberg", "id", "9", "popularity", "5"));
    assertU(commit());
  }



  

  

  
}
