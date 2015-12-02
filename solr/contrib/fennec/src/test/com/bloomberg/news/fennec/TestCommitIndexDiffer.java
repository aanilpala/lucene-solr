import com.bloomberg.news.fennec.common.DocumentFrequencyUpdate;
import com.yammer.metrics.Metrics;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

// Class will test the solr to kafka side of the system
public class TestCommitIndexDiffer extends SolrTestCaseJ4 {

    @BeforeClass
    public static void beforeClass() throws Exception {
    }

    @AfterClass
    public static void clearVariables() {
    }

    public static SolrInputDocument getDocument() {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", 0);
        doc.addField("name", "Fred");
        doc.addField("text", "some text");

        return doc;
    }

    /**
     * Commit changes, note that the harness's commit doesn't actually commit
     */
    private void commit() {
        h.update("<commit></commit>");
    }

    /**
     * Delete documents by a query
     * @param query
     */
    private void deleteByQuery(String query) {
        StringBuilder sb = new StringBuilder("<delete><query>");
        sb.append(query);
        sb.append("</query></delete>");
        h.update(sb.toString());
    }
    /**
     * Construct the document in XML format (for our simple docs with simple fields and values)
     * @param doc
     */
    private void add(SolrInputDocument doc) {
        StringBuilder sb = new StringBuilder();
        sb.append("<add><doc>");
        for (String field : doc.getFieldNames()) {
            sb.append( "<field name=\"");
            sb.append(field);
            sb.append("\">");
            sb.append(doc.getFieldValue(field));
            sb.append("</field>");
        }

        sb.append("</doc></add>");

        h.update(sb.toString());
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        initCore("./solr/collection1/conf/solrconfig.xml", "./solr/collection1/conf/schema.xml");
    }

    @After
    public void cleanup() throws Exception {
        h.getCoreContainer().shutdown();
        MockDocumentFrequencyUpdateEventListener.cleanup();
        Metrics.shutdown();
    }

    public void clearIndex() {
        try {
            h.update("<delete><query>*:*</query></delete>");
        } catch (Exception e) {
        }
    }

    @Test
    public void testSingleAdd() throws IOException, SolrServerException {
        clearIndex();
        add(getDocument());
        commit();

        Map<String, List<DocumentFrequencyUpdate>> updates = MockDocumentFrequencyUpdateEventListener.getLastUpdate();
        assertEquals(3, updates.keySet().size());
        for (String key : updates.keySet()) {
            for (DocumentFrequencyUpdate update : updates.get(key)) {
                assertEquals(1, update.docFreq);
            }
        }
    }

    @Test
    public void testCommitUpdates() throws IOException, SolrServerException {
        clearIndex();
        SolrInputDocument doc = getDocument();
        add(doc);
        commit();
        doc.setField("name", "John");
        add(doc);
        commit();
        Map<String, List<DocumentFrequencyUpdate>> updates = MockDocumentFrequencyUpdateEventListener.getLastUpdate();

        assertEquals(updates.keySet().size(), 3);
        for (String key : updates.keySet()) {
            for (DocumentFrequencyUpdate update : updates.get(key)) {
                if (update.term.equalsIgnoreCase("John")) {
                    assertEquals(1, update.docFreq);
                } else {
                    assertEquals(2, update.docFreq);
                }
            }
        }

    }

    @Test
    public void testDeleteByQuery() throws IOException, SolrServerException {
        clearIndex();
        SolrInputDocument doc=  getDocument();
        for (int i = 0; i < 10000; i++) {
            doc.setField("id", i);
            add(doc);
        }

        commit();
        Map<String, List<DocumentFrequencyUpdate>> updates = MockDocumentFrequencyUpdateEventListener.getLastUpdate();

        assertEquals(3, updates.keySet().size());
        assertEquals(10000, updates.get("id").size());
        assertEquals(1, updates.get("name").size());
        assertEquals(10000, updates.get("name").get(0).docFreq);

        deleteByQuery("id:5");
        commit();
        //Not merged
        updates = MockDocumentFrequencyUpdateEventListener.getLastUpdate();
        assertEquals(3, updates.keySet().size());
        assertEquals(0, updates.get("id").size());
        assertEquals(0, updates.get("text").size());

        deleteByQuery("*:*");
        commit();
        updates = MockDocumentFrequencyUpdateEventListener.getLastUpdate();
        assertEquals(3, updates.keySet().size());
        assertEquals(10000, updates.get("id").size());
        assertEquals(1, updates.get("text").size());
        assertEquals(0, updates.get("name").get(0).docFreq);
    }

    @Test
    public void testStressDeleteDelayedCommit() throws IOException, SolrServerException {
        clearIndex();
        SolrInputDocument doc=  getDocument();
        for (int i = 0; i < 10000; i++) {
            doc.setField("id", i);
            add(doc);
        }

        commit();
        Map<String, List<DocumentFrequencyUpdate>> updates = MockDocumentFrequencyUpdateEventListener.getLastUpdate();

        assertEquals(3, updates.keySet().size());
        assertEquals(10000, updates.get("id").size());
        assertEquals(1, updates.get("name").size());
        assertEquals(10000, updates.get("name").get(0).docFreq);
        deleteByQuery("id:5");
        for (int i = 10000; i < 10010; i++) {
            doc.setField("id", i);
            add(doc);
        }
        commit();
        updates = MockDocumentFrequencyUpdateEventListener.getLastUpdate();
        assertEquals(3, updates.keySet().size());
        assertEquals(10, updates.get("id").size());
        assertEquals(1, updates.get("text").size());
        assertEquals(10010, updates.get("name").get(0).docFreq);
    }

    @Test
    public void testDeleteByQueryLargeDictionary() throws IOException, SolrServerException {
        clearIndex();
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(new FileInputStream(getFile("lotsOfWords.txt"))));
        String[] content = reader.readLine().split(" ");
        SolrInputDocument doc = getDocument();

        for (int i =0; i < content.length; i++) {
            doc.setField("text", content[i]);
            doc.setField("id", i);
            add(doc);
        }

        commit();
        Map<String, List<DocumentFrequencyUpdate>> updates = MockDocumentFrequencyUpdateEventListener.getLastUpdate();

        assertEquals(3, updates.keySet().size());
        assertEquals(580, updates.get("id").size());
        assertEquals(580, updates.get("text").size());
        assertEquals(580, updates.get("name").get(0).docFreq);

        deleteByQuery("id:5");
        commit();
        // Not merged yet and we cannot force a merge from here
        updates = MockDocumentFrequencyUpdateEventListener.getLastUpdate();
        assertEquals(3, updates.keySet().size());
        assertEquals(0, updates.get("id").size());
        assertEquals(0, updates.get("name").size());

        deleteByQuery("*:*");
        commit();
        updates = MockDocumentFrequencyUpdateEventListener.getLastUpdate();
        assertEquals(3, updates.keySet().size());
        assertEquals(580, updates.get("id").size());
        assertEquals(580, updates.get("text").size());
        assertEquals(0, updates.get("name").get(0).docFreq);

    }

    @Test
    public void testClearIndexChangeFields() throws IOException, SolrServerException {
        clearIndex();
        SolrInputDocument doc = getDocument();

        // Added 50 docs with 3 fields
        for (int i =0; i < 50; i++) {
            doc.setField("id", i);
            add(doc);
        }
        commit();
        Map<String, List<DocumentFrequencyUpdate>> updates = MockDocumentFrequencyUpdateEventListener.getLastUpdate();

        assertEquals(3, updates.keySet().size());
        assertEquals(50, updates.get("id").size());
        assertEquals(1, updates.get("text").size());
        assertEquals(50, updates.get("name").get(0).docFreq);

        clearIndex();
        doc.removeField("text");
        // add 50 docs with 2 fields
        for (int i =50; i < 100; i++) {
            doc.setField("id", i);
            add(doc);
        }
        commit();
        updates = MockDocumentFrequencyUpdateEventListener.getLastUpdate();

        // Not merged, so still 3 but name docFreq didn't change. Instead we have ids changed, and text changed
        assertEquals(3, updates.keySet().size());
        assertEquals(100, updates.get("id").size());
        assertEquals(0, updates.get("text").get(0).docFreq);
    }

    @Test
    public void testNoChange() throws IOException, SolrServerException {
        clearIndex();
        SolrInputDocument doc = getDocument();

        for (int i =0; i < 50; i++) {
            doc.setField("id", i);
            add(doc);
        }
        commit();
        Map<String, List<DocumentFrequencyUpdate>> earlierUpdate= MockDocumentFrequencyUpdateEventListener.getLastUpdate();

        commit();
        Map<String, List<DocumentFrequencyUpdate>> updates = MockDocumentFrequencyUpdateEventListener.getLastUpdate();

        // It seems that empty commits do not create a new
        for (String key : earlierUpdate.keySet()) {
            List<DocumentFrequencyUpdate> earlierUpdates = earlierUpdate.get(key);
            List<DocumentFrequencyUpdate> newUpdates = updates.get(key);
            for (int i =0; i< earlierUpdates.size(); i++) {
                assertEquals(earlierUpdates.get(i).docFreq, newUpdates.get(i).docFreq);
            }
        }
    }

}

