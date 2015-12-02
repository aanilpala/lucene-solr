import com.bloomberg.news.fennec.common.DocumentFrequencyUpdate;
import com.bloomberg.news.fennec.solr.AbstractDocumentFrequencyUpdateEventListener;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Mock event listener for unit tests
 */
public class MockDocumentFrequencyUpdateEventListener extends AbstractDocumentFrequencyUpdateEventListener {
    public static Map<String, List<DocumentFrequencyUpdate>> lastUpdate = Collections.emptyMap();

    @Override
    public void init(NamedList args) {
        super.init(args);
        // Set the diffInterval to 0 because we want to diff everytime for the tests
        this.diffInterval = 0;
    }

    public MockDocumentFrequencyUpdateEventListener(SolrCore core) throws IOException {
        super(core);
    }

    @Override
    protected void updateDocumentFrequency(Map<String, List<DocumentFrequencyUpdate>> updates) {
        lastUpdate = updates;
    }

    public static Map<String, List<DocumentFrequencyUpdate>> getLastUpdate() {
        return lastUpdate;
    }

    public static void cleanup() {
        lastUpdate = Collections.emptyMap();
    }
}