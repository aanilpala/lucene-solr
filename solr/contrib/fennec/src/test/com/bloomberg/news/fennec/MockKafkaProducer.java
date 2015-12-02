import com.bloomberg.news.fennec.solr.DocumentFrequencyUpdateProducer;
import com.bloomberg.news.fennec.common.DocumentFrequencyUpdate;

import java.util.HashMap;
import java.util.List;

/**
 * Mock the producer for unit tests
 */
public class MockKafkaProducer extends DocumentFrequencyUpdateProducer{
    public HashMap<String, List<DocumentFrequencyUpdate>> lastUpdate;
    private static MockKafkaProducer producer = null;

    public static MockKafkaProducer getProducer() {
        if (producer == null) {
            producer = new MockKafkaProducer();
        }

        return producer;
    }

    private MockKafkaProducer() {}

    // Override this method because we want to see what happens
    @Override
    public void updateDocumentFrequency(HashMap<String, List<DocumentFrequencyUpdate>> updateMap) {
        lastUpdate = updateMap;
    }

    public HashMap<String, List<DocumentFrequencyUpdate>> getLastUpdate() {
        return lastUpdate;
    }

}