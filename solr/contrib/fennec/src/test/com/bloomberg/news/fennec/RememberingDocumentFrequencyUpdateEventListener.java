package com.bloomberg.news.fennec;

import com.bloomberg.news.fennec.common.DocumentFrequencyUpdate;
import com.bloomberg.news.fennec.solr.AbstractDocumentFrequencyUpdateEventListener;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Mock event listener for unit tests
 */
public class RememberingDocumentFrequencyUpdateEventListener extends AbstractDocumentFrequencyUpdateEventListener {
    public Map<String, List<DocumentFrequencyUpdate>> lastUpdate;

    @Override
    public void init(NamedList args) {
        super.init(args);
    }

    public RememberingDocumentFrequencyUpdateEventListener(SolrCore core) throws IOException {
        super(core);
    }

    @Override
    protected void updateDocumentFrequency(Map<String, List<DocumentFrequencyUpdate>> updates) {
        lastUpdate = updates;
    }

    public Map<String, List<DocumentFrequencyUpdate>> getLastUpdate() {
        return lastUpdate;
    }

    @Override
    public void handleDiffFuture(Future<?> task) {
        // We want to block on this the task's execution for our testing purpose
        try {
            task.get();
        } catch (InterruptedException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Diff thread interrupted.",e);
        } catch (ExecutionException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error while performing index commit diff", e);
        }
    }

    public void cleanup() {
        lastUpdate = null;
    }
}
