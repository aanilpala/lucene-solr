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
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.index.IndexCommit;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * A solr request handler for triggering a full flush of document frequency updates
 * Very similar to the DocumentFrequencyUpdateEventListener, except that because the event listener is
 * called by DirectUpdateHandler2 inside of commit, we cannot have it used an endpoint to be called to flush
 */
public class DocumentFrequencyFlushRequestHandler extends RequestHandlerBase {

    private static final Logger log = LoggerFactory.getLogger(DocumentFrequencyFlushRequestHandler.class);

    private static final String COLLECTION_NAME_PARAM = "collectionName";

    private CoreContainer coreContainer;
    private DocumentFrequencyUpdateProducer producer;
    private Set<String> fieldsToDiff;

    public DocumentFrequencyFlushRequestHandler() {}

    public DocumentFrequencyFlushRequestHandler(CoreContainer container) {
        this.coreContainer = container;
    }

    /**
     * We need to initialize both the producer and also the field set to pass to the differ
     * note one issue here is that we basically setup the EventListener again...
     */
    @Override
    public void init(NamedList args) {
        String propertiesFile = (String) args.get(FennecConstants.PROPERTIES_FILE_KEY);
        log.info("Initializing DocumentFrequencyFlushRequestHandler");

        try {
            Properties props = new Properties();
            props.load(new FileInputStream(propertiesFile));
            String fields = (String) props.get(FennecConstants.FIELDS_KEY);
            log.info("Event listener configured to diff fields: ", fields);
            if (fields != null) {
                this.fieldsToDiff = new HashSet<>();
                Collections.addAll(this.fieldsToDiff, fields.split(FennecConstants.SEPARATOR));
            } else {
                this.fieldsToDiff = null;
            }

            this.producer= new DocumentFrequencyUpdateProducer(propertiesFile);
        } catch (IOException e) {
            log.error("Unableld to initialize DocumentFrequencyFlushHandler", e);
        }
    }

    @Override
    public void handleRequestBody(SolrQueryRequest solrQueryRequest, SolrQueryResponse solrQueryResponse) {
        String collectionName = solrQueryRequest.getParams().get(COLLECTION_NAME_PARAM, StringUtils.EMPTY);

        // The collectionName must be specified, or we will not do anything
        long startTime = System.currentTimeMillis();
        List<String> coresFlushed = new ArrayList<>();
        if (collectionName.isEmpty()) {
            log.error("Required collectionName parameter missing for flush document frequencies");
            solrQueryResponse.add("Error", "Missing required parameter collectionName");
            solrQueryResponse.add("Status", "Error");
            return;
        } else {
            for (SolrCore core : this.coreContainer.getCores()) {
                if (core.getCoreDescriptor().getCollectionName().equals(collectionName) &&flushCore(core)) {
                    coresFlushed.add(core.getCoreDescriptor().getName());
                }
            }
        }
        solrQueryResponse.add("Cores", coresFlushed);
        solrQueryResponse.add("Status", "Completed");
        log.debug("Flushing cores completed, took {} miliseconds", System.currentTimeMillis() - startTime);
    }

    /**
     * Method called by handleRequestBody so that we can flush all the document frequencies from a core to the store
     * @param core SolrCore to flush, the core MUST NOT be null
     * @return Whether a flush was completed or not
     * @throws IOException
     */
    private boolean flushCore(SolrCore core){
        CloudDescriptor cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
        String shardId, collectionName;

        if (cloudDescriptor == null) {
            shardId = DocumentFrequencyUpdate.DEFAULT_SHARD;
            collectionName = core.getCoreDescriptor().getCollectionName();
        } else {
            shardId = cloudDescriptor.getShardId();
            collectionName = cloudDescriptor.getCollectionName();
        }

        // Perform the flush only if this is core is not in a solr cloud application or is the leader of the shard
        if (cloudDescriptor == null || cloudDescriptor.isLeader()) {

            IndexCommit commit = core.getDeletionPolicy().getLatestCommit();
            long commitGen = commit.getGeneration();
            // Must save also, even though at this point
            core.getDeletionPolicy().saveCommitPoint(commitGen);

            HashMap<String, List<DocumentFrequencyUpdate>> updates =
                    null;
            try {
                updates = DocumentFrequencyIndexDiffer.diffCommits(null, commit, shardId, collectionName, this.fieldsToDiff);
                core.getDeletionPolicy().releaseCommitPoint(commitGen);
            } catch (IOException e) {
                log.error("Unable to flush document frequencies for collection {}, shard {}", collectionName, shardId);
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error whiling performing diff", e);
            } finally {
                // Cleanup
                core.getDeletionPolicy().releaseCommitPoint(commitGen);
            }

            this.producer.updateDocumentFrequency(updates);
            // Update completed
            return true;
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
