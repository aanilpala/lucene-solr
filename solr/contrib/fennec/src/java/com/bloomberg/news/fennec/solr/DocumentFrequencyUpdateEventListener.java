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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.AbstractSolrEventListener;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * EventHandler that can be specified in solrconfig.xml to perform diffing after each commit
 * Need to be used in conjunction with the com.bloomberg.news.fennec.solr.DocumentFrequencyUpdateDeletionPolicy
 * or there would be severe performance issues because we will publish all doc freqs each time
 * During the postCommit callback, only the leader of a shard will do any diffing
 */
public class DocumentFrequencyUpdateEventListener extends AbstractSolrEventListener{

    private static final Logger log = LoggerFactory.getLogger(DocumentFrequencyUpdateEventListener.class);

    // Differ doesn't store any state so the fields to diff on are here
    private Set<String> fieldsToDiff;
    protected DocumentFrequencyUpdateProducer producer;
    protected String propertiesFile;

    /**
     * Constructor called during solr initialization
     * @param core
     * @throws IOException
     */
    public DocumentFrequencyUpdateEventListener(SolrCore core) throws IOException {
        super(core);
    }

    @Override
    public void init(NamedList args) {
        this.propertiesFile = (String) args.get(FennecConstants.PROPERTIES_FILE_KEY);
        log.info("Initializing Event Listener");
        try {
            Properties props = new Properties();
            props.load(new FileInputStream(this.propertiesFile));
            String fields = (String) props.get(FennecConstants.FIELDS_KEY);
            log.info("Event listener configured to diff fields: ", fields);
            if (fields != null) {
                this.fieldsToDiff = new HashSet<>();
                Collections.addAll(this.fieldsToDiff, fields.split(FennecConstants.SEPARATOR));
            } else {
                this.fieldsToDiff = null;
            }

            this.producer= new DocumentFrequencyUpdateProducer(propertiesFile);
            log.info("Finished initializing event listener");
        } catch (IOException e) {
            log.error("Unable to initialize kafka producer");
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to initialize Event Listener", e);
        }

    }

    @Override
    public void postCommit() {
        // Method is called within DirectUpdateHandler2's commit method
        // from inside the commitLock critical section
        SolrCore core = this.getCore();
        CloudDescriptor cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();

        String shardId, collectionName;
        if (cloudDescriptor == null) {
            shardId = DocumentFrequencyUpdate.DEFAULT_SHARD;
            collectionName = core.getCoreDescriptor().getCollectionName();
        } else {
            shardId = cloudDescriptor.getShardId();
            collectionName = cloudDescriptor.getCollectionName();
        }

        // If we are not a solrcloud node
        // or that we are, AND we are the leader
        // diff the indices and produce logs for kafka
        if ( cloudDescriptor == null || cloudDescriptor.isLeader()) {
            Map<Long, IndexCommit> commits = core.getDeletionPolicy().getCommits();
            // Now iterate through the commits
            ArrayList<Long> sortedGenerationNums = new ArrayList<>();
            sortedGenerationNums.addAll(commits.keySet());
            Collections.sort(sortedGenerationNums);
            long newestCommitGen = -1, earlierCommitGen = -1;
            try {
                // Now that the commits are sorted, we want to reserve them
                // We also only care about the last 2

                long startTime = System.currentTimeMillis();
                if (sortedGenerationNums.size() == 1 ) {
                    newestCommitGen = sortedGenerationNums.get(0);
                    core.getDeletionPolicy().saveCommitPoint(newestCommitGen);

                    IndexCommit newestCommit = core.getDeletionPolicy().getCommitPoint(newestCommitGen);
                    HashMap<String, List<DocumentFrequencyUpdate>> updates =
                            DocumentFrequencyIndexDiffer.diffCommits(null, newestCommit, shardId, collectionName, this.fieldsToDiff);

                    this.producer.updateDocumentFrequency(updates);
                    core.getDeletionPolicy().releaseCommitPoint(newestCommitGen);
                } else if (sortedGenerationNums.size() > 1) {
                    newestCommitGen = sortedGenerationNums.get(sortedGenerationNums.size() - 1);
                    earlierCommitGen = sortedGenerationNums.get(sortedGenerationNums.size() - 2);

                    // Save the commit points so that the commits won't be deleted by something else until we are done
                    core.getDeletionPolicy().saveCommitPoint(newestCommitGen);
                    core.getDeletionPolicy().saveCommitPoint(earlierCommitGen);

                    IndexCommit newerCommit = core.getDeletionPolicy().getCommitPoint(newestCommitGen);
                    IndexCommit earlierCommit = core.getDeletionPolicy().getCommitPoint(earlierCommitGen);

                    HashMap<String, List<DocumentFrequencyUpdate>> updates =
                            DocumentFrequencyIndexDiffer.diffCommits(earlierCommit, newerCommit, shardId, collectionName, this.fieldsToDiff);
                    this.producer.updateDocumentFrequency(updates);

                    // MUST RELEASE
                    core.getDeletionPolicy().releaseCommitPoint(newestCommitGen);
                    core.getDeletionPolicy().releaseCommitPoint(earlierCommitGen);
                }
                log.debug("Listener completed update in {} miliseconds", System.currentTimeMillis() - startTime);
            } catch (IOException e) {
                // We are not allowed to throw exceptions here so will catch this
                throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Error pushing diff to kafka", e);
            } finally {
                core.getDeletionPolicy().releaseCommitPoint(newestCommitGen);
                if (earlierCommitGen >= 0) core.getDeletionPolicy().releaseCommitPoint(earlierCommitGen);
            }

        }
    }

    @Override
    public void postSoftCommit() {

    }

    @Override
    public void newSearcher(SolrIndexSearcher solrIndexSearcher, SolrIndexSearcher solrIndexSearcher1) {

    }
}
