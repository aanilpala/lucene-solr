package com.bloomberg.news.fennec;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.bloomberg.news.fennec.common.DocumentFrequencyUpdate;

// Class will test the solr to kafka side of the system
public class TestAbstractDocumentFrequencyUpdateEventListener extends SolrTestCaseJ4 {
  
    RememberingDocumentFrequencyUpdateEventListener updateEventListener;

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
     */
    private void deleteByQuery(String query) {
        StringBuilder sb = new StringBuilder("<delete><query>");
        sb.append(query);
        sb.append("</query></delete>");
        h.update(sb.toString());
    }
    /**
     * Construct the document in XML format (for our simple docs with simple fields and values)
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
        initCore("solrconfig.xml", "schema.xml", getFile("fennec/solr").getAbsolutePath());
        updateEventListener = new RememberingDocumentFrequencyUpdateEventListener(h.getCore());
        final NamedList args = new NamedList<>();
        args.add("diff.interval.ms", 0);
        updateEventListener.init(args);
        h.getCore().getUpdateHandler().registerCommitCallback(updateEventListener);
    }

    @After
    public void cleanup() throws Exception {
        h.getCoreContainer().shutdown();
        updateEventListener.cleanup();
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

        Map<String, List<DocumentFrequencyUpdate>> updates = updateEventListener.getLastUpdate();
        assertNotNull(updates);
        assertEquals(3, updates.keySet().size());
        for (String key : updates.keySet()) {
            for (DocumentFrequencyUpdate update : updates.get(key)) {
                assertEquals(1, update.getDocumentFrequency());
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
        Map<String, List<DocumentFrequencyUpdate>> updates = updateEventListener.getLastUpdate();

        assertNotNull(updates);
        assertEquals(updates.keySet().size(), 3);
        for (String key : updates.keySet()) {
            for (DocumentFrequencyUpdate update : updates.get(key)) {
                if (update.getTerm().equalsIgnoreCase("John")) {
                    assertEquals(1, update.getDocumentFrequency());
                } else {
                    assertEquals(2, update.getDocumentFrequency());
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
        Map<String, List<DocumentFrequencyUpdate>> updates = updateEventListener.getLastUpdate();

        assertNotNull(updates);
        assertEquals(3, updates.keySet().size());
        assertEquals(10000, updates.get("id").size());
        assertEquals(1, updates.get("name").size());
        assertEquals(10000, updates.get("name").get(0).getDocumentFrequency());
        updateEventListener.cleanup();

        deleteByQuery("id:5");
        commit();
        //Not merged
        updates = updateEventListener.getLastUpdate();
        assertNotNull(updates);
        assertEquals(3, updates.keySet().size());
        assertEquals(0, updates.get("id").size());
        assertEquals(0, updates.get("text").size());
        updateEventListener.cleanup();

        deleteByQuery("*:*");
        commit();
        updates = updateEventListener.getLastUpdate();
        assertNotNull(updates);
        assertEquals(3, updates.keySet().size());
        assertEquals(10000, updates.get("id").size());
        assertEquals(1, updates.get("text").size());
        assertEquals(0, updates.get("name").get(0).getDocumentFrequency());
    }

    @Test
    public void testStressDeleteDelayedCommit() throws IOException, SolrServerException {
        clearIndex();
        SolrInputDocument doc = getDocument();
        for (int i = 0; i < 10000; i++) {
            doc.setField("id", i);
            add(doc);
        }
        
        commit();        
        
        Map<String, List<DocumentFrequencyUpdate>> updates = updateEventListener.getLastUpdate();
        
        assertNotNull(updates);
        assertEquals(3, updates.keySet().size());
        assertEquals(10000, updates.get("id").size());
        assertEquals(1, updates.get("name").size());
        assertEquals(10000, updates.get("name").get(0).getDocumentFrequency());
        updateEventListener.cleanup();
        
        deleteByQuery("id:5");
        
        for (int i = 10000; i < 10010; i++) {
            doc.setField("id", i);
            add(doc);
        }
        
        commit();
        
        updates = updateEventListener.getLastUpdate();
        assertNotNull(updates);
        assertEquals(3, updates.keySet().size());
        assertEquals(10, updates.get("id").size());
        assertEquals(1, updates.get("text").size());
        assertEquals(10010, updates.get("name").get(0).getDocumentFrequency());
    }

    @Test
    public void testDeleteByQueryLargeDictionary() throws IOException, SolrServerException {
        clearIndex();
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(new FileInputStream(getFile("lotsOfWords.txt"))));
        String[] content = reader.readLine().split(" ");
        reader.close();
        SolrInputDocument doc = getDocument();
        
        for (int i =0; i < content.length; i++) {
            doc.setField("text", content[i]);
            doc.setField("id", i);
            add(doc);
        }
        
        commit();
        
        Map<String, List<DocumentFrequencyUpdate>> updates = updateEventListener.getLastUpdate();

        assertNotNull(updates);
        assertEquals(3, updates.keySet().size());
        assertEquals(580, updates.get("id").size());
        assertEquals(580, updates.get("text").size());
        assertEquals(580, updates.get("name").get(0).getDocumentFrequency());
        updateEventListener.cleanup();

        deleteByQuery("id:5");
        commit();
        
        // Not merged yet and we cannot force a merge from here
        updates = updateEventListener.getLastUpdate();
        assertNotNull(updates);
        assertEquals(3, updates.keySet().size());
        assertEquals(0, updates.get("id").size());
        assertEquals(0, updates.get("name").size());
        updateEventListener.cleanup();

        deleteByQuery("*:*");
        commit();
        
        updates = updateEventListener.getLastUpdate();
        assertNotNull(updates);
        assertEquals(3, updates.keySet().size());
        assertEquals(580, updates.get("id").size());
        assertEquals(580, updates.get("text").size());
        assertEquals(0, updates.get("name").get(0).getDocumentFrequency());
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
        Map<String, List<DocumentFrequencyUpdate>> updates = updateEventListener.getLastUpdate();

        assertEquals(3, updates.keySet().size());
        assertEquals(50, updates.get("id").size());
        assertEquals(1, updates.get("text").size());
        assertEquals(50, updates.get("name").get(0).getDocumentFrequency());

        clearIndex();
        doc.removeField("text");
        // add 50 docs with 2 fields
        for (int i =50; i < 100; i++) {
            doc.setField("id", i);
            add(doc);
        }
        commit();
        updates = updateEventListener.getLastUpdate();

        // Not merged, so still 3 but name getDocumentFrequency() didn't change. Instead we have ids changed, and text changed
        assertEquals(3, updates.keySet().size());
        assertEquals(100, updates.get("id").size());
        assertEquals(0, updates.get("text").get(0).getDocumentFrequency());
    }

    @Test
    public void testNoChange() throws IOException, SolrServerException {
        clearIndex();
        SolrInputDocument doc = getDocument();

        for (int i = 0; i < 50; i++) {
            doc.setField("id", i);
            add(doc);
        }

        commit();
        final Map<String, List<DocumentFrequencyUpdate>> earlierUpdate= updateEventListener.getLastUpdate();
        assertTrue(earlierUpdate.size() > 0);
        updateEventListener.cleanup();

        commit();
        final Map<String, List<DocumentFrequencyUpdate>> updates = updateEventListener.getLastUpdate();
        assertEquals(3, updates.size());
        for (List<DocumentFrequencyUpdate> updateList : updates.values()) {
            assertTrue(updateList.isEmpty());
        }
        updateEventListener.cleanup();
    }
}

