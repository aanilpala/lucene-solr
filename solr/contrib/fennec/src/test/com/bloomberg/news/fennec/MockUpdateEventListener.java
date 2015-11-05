import com.bloomberg.news.fennec.solr.DocumentFrequencyUpdateEventListener;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;

import java.io.IOException;

/**
 * Mock event listener for unit tests
 */
public class MockUpdateEventListener extends DocumentFrequencyUpdateEventListener{
    public MockUpdateEventListener(SolrCore core) throws IOException {
        super(core);
    }

    @Override
    public void init (NamedList args) {
        this.producer = MockKafkaProducer.getProducer();
    }
}
