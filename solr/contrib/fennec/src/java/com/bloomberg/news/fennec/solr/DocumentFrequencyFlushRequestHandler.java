package com.bloomberg.news.fennec.solr;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.bloomberg.news.fennec.common.DocumentFrequencyUpdate;
import com.bloomberg.news.fennec.common.FennecConstants;

import org.apache.lucene.index.IndexCommit;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;

/**
 * A solr request handler for triggering a full flush of document frequency updates
 * Very similar to the KafkaDocumentFrequencyUpdateEventListener, except that because the event listener is
 * called by DirectUpdateHandler2 inside of commit, we cannot have it used an endpoint to be called to flush
 */
public class DocumentFrequencyFlushRequestHandler extends RequestHandlerBase {

    private static final Logger log = LoggerFactory.getLogger(DocumentFrequencyFlushRequestHandler.class);

    private KafkaDocumentFrequencyUpdateProducer producer;
    private Set<String> fieldsToDiff;

    public DocumentFrequencyFlushRequestHandler() {
        super();
    }

    /**
     * We need to initialize both the producer and also the field set to pass to the differ
     * note one issue here is that we basically setup the EventListener again...
     */
    @Override
    public void init(NamedList args) {
        log.info("Initializing DocumentFrequencyFlushRequestHandler");

        String fields = (String) args.get(FennecConstants.FIELDS_KEY);
        log.info("Event listener configured to diff fields: {}", fields);
        if (fields != null) {
            this.fieldsToDiff = new HashSet<String>(Arrays.asList(fields.split(FennecConstants.SEPARATOR)));
        }
        log.info("Flushing configured for fields: {}", fields);
        this.producer = new KafkaDocumentFrequencyUpdateProducer(args);
        log.info("Successfully initialized flush request handler");
    }

    @Override
    public void handleRequestBody(SolrQueryRequest solrQueryRequest, SolrQueryResponse solrQueryResponse) {
        long startTime = System.nanoTime();
        final SolrCore core = solrQueryRequest.getCore();
        
        if (core == null) {
            log.warn("No core is associated with the request so cannot flush document frequencies");
            solrQueryResponse.add("Status", "Failed");
            return;
        }
        
        if (flushCore(core)) {
            solrQueryResponse.add("Status", "Completed");
        }
        else
        {
            solrQueryResponse.add("Status", "Failed");
        }
        
        solrQueryResponse.add("Core", core);
        log.debug("Flushing cores completed, took {} miliseconds", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
    }

    /**
     * Method called by handleRequestBody so that we can flush all the document frequencies from a core to the store
     * @param core SolrCore to flush, the core MUST NOT be null
     * @return Whether a flush was completed or not
     */
    private boolean flushCore(SolrCore core){
        final CloudDescriptor cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
        final String collectionName, shardId;

        if (cloudDescriptor != null) {
            collectionName = cloudDescriptor.getCollectionName();
            shardId = cloudDescriptor.getShardId();
        } else {
            collectionName = null;
            shardId = null;
        }

        // Perform the flush only if this core is not in a solr cloud application or is the leader of the shard
        if (cloudDescriptor == null || cloudDescriptor.isLeader()) {
          
            final long commitGen = core.getDeletionPolicy().getLatestCommit().getGeneration();
            core.getDeletionPolicy().saveCommitPoint(commitGen);
            final IndexCommit commit = core.getDeletionPolicy().getCommitPoint(commitGen);
            if (commit == null) {
                log.info("could not retreive commit gen {}", commitGen);
                return false;
            }
            
            log.info("flushing core={} commitgen={} commit={} fieldsToDiff={}", core, commitGen, commit, fieldsToDiff);

            Map<String, List<DocumentFrequencyUpdate>> updates = Collections.emptyMap();
            try {
                updates = DocumentFrequencyIndexDiffer.diffCommits(null, commit, this.fieldsToDiff, collectionName, shardId);
                producer.updateDocumentFrequency(updates);
                return true;
            } finally {
                core.getDeletionPolicy().releaseCommitPoint(commitGen);
            }
        }

        // We did nothing
        return false;
    }

    @Override
    public String getDescription() {
        return "Handler will trigger a full flush of all term frequencies currently in the index to the DocumentFrequencyStore";
    }

    @Override
    public String getSource() {
        return "bbgithub:news/lucene-solr";
    }
}
