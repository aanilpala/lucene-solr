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
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class that performs the logic of the event listener,
 * but leaves how it is published out up to the implementation class
 * We decided to have different types of publishers implement this class
 * instead of passing in a producer as parameter because we decided
 * the inheritance hierarchy was cleaner than trying to use reflection here.
 * especially since different publisher/producers might have different
 * configuration options, eg a Kafka producer needs a bunch of ports/host/brokers
 * vs a publisher that just uses the slf4j logs doesn't need any specified here
 *
 *
 * EventHandler that can be specified in solrconfig.xml to perform diffing after each commit
 * Need to be used in conjunction with the com.bloomberg.news.fennec.solr.DocumentFrequencyUpdateDeletionPolicy
 * or there would be severe performance issues because we will publish all doc freqs each time
 * During the postCommit callback, only the leader of a shard will do any diffing
 */
abstract public class AbstractDocumentFrequencyUpdateEventListener extends AbstractSolrEventListener {
    private static final Logger log = LoggerFactory.getLogger(AbstractDocumentFrequencyUpdateEventListener.class);
    private static final int DEFAULT_DIFF_INTERVAL_MS = 1000;
    private static final int DEFAULT_SHUTDOWN_TIME_SECONDS = 1;
    // NOTE we can only have 1 thread in the thread pool, because if we have more than one, we cannot
    // guarantee the order of updates when we publish to the store
    private static final int DIFF_THREAD_LIMIT = 1;

    // Supply the value in MILLISECONDS
    private static final String DIFF_INTERVAL = "diff.interval";

    // Differ doesn't store any state so the fields to diff on are here
    protected Set<String> fieldsToDiff;
    private long lastDiffEpoch;
    protected int diffInterval;

    protected ThreadPoolExecutor executor;

    /**
     * The task that contains the logic for performing the diff and also publishing
     * Should be submitted to the thread pool.
     */
    protected class DiffTask implements Runnable {
        private IndexCommit olderCommit;
        private IndexCommit newerCommit;
        private long olderGenNum;
        private long newerGenNum;

        private String shardId;
        private String collectionName;
        private IndexDeletionPolicyWrapper deletionPolicy;
        // Nullable, null fieldsToDiff indicate a diff on all fields
        private final Set<String> fieldsToDiff;

        public DiffTask(IndexCommit olderCommit, IndexCommit newerCommit,
                        long olderGenNum, long newerGenNum, String shardId,
                        String collectionName, Set<String> diffFields,
                        IndexDeletionPolicyWrapper deletionPolicy) {
            this.olderCommit = olderCommit;
            this.newerCommit = newerCommit;
            this.olderGenNum = olderGenNum;
            this.newerGenNum = newerGenNum;
            this.shardId = shardId;
            this.collectionName = collectionName;
            this.fieldsToDiff = diffFields;
            this.deletionPolicy = deletionPolicy;
        }

        @Override
        public void run() {
            long startTime = System.nanoTime();
            try {
                Map<String, List<DocumentFrequencyUpdate>> updates =
                        DocumentFrequencyIndexDiffer.diffCommits(olderCommit, newerCommit, shardId, collectionName, fieldsToDiff);

                updateDocumentFrequency(updates);
            }catch (IOException e) {
                // We are not allowed to throw exceptions here so will catch this
                throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Error publishing diff", e);
            } finally {
                deletionPolicy.releaseCommitPoint(newerGenNum);
                if (olderGenNum >= 0) deletionPolicy.releaseCommitPoint(olderGenNum);
            }
            log.debug("Listener completed update in {} miliseconds", (System.nanoTime() - startTime) /1000 );
        }
    }

    /**
     * Implementing classes may need to supply their own close hooks that shutdown external publishers
     */
    protected class EventListenerCloseHook extends CloseHook {

        @Override
        public void preClose(SolrCore core) {
            executor.shutdown();
        }

        @Override
        public void postClose(SolrCore core) {
            if (!executor.isTerminated()) {
                executor.shutdown();
                try {
                    executor.awaitTermination(DEFAULT_SHUTDOWN_TIME_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    log.warn("Executor failed to shutdown in {} seconds, forcing shutdown" , DEFAULT_DIFF_INTERVAL_MS);
                    executor.shutdownNow();
                }
            }
        }
    }

    public AbstractDocumentFrequencyUpdateEventListener(SolrCore core) {
        super(core);
        this.diffInterval = DEFAULT_DIFF_INTERVAL_MS;
        // Set to 0 because we haven't done a diff yet
        this.lastDiffEpoch = 0;
        this.fieldsToDiff = null;
        // ThreadPoolExecutor is the implementation used by executors
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(DIFF_THREAD_LIMIT);

        // We need to add a close hook to Solr because we need to shutdown our executor service
       registerCloseHook();
    }

    @Override
    public void init(NamedList args) {
        log.info("Initializing Abstract Event Listener");
        if (args.get(DIFF_INTERVAL) != null) {
            this.diffInterval = (int) args.get(DIFF_INTERVAL);
        }
        log.info("Arguments: {}", args);

        String fields = (String) args.get(FennecConstants.FIELDS_KEY);
        log.info("Event listener configured to diff fields: {}", fields);
        if (fields != null && ! fields.isEmpty()) {
            this.fieldsToDiff = new HashSet<>();
            Collections.addAll(this.fieldsToDiff, fields.split(FennecConstants.SEPARATOR));
        }
        log.info("Finished initializing abstract event listener");
    }

    @Override
    public void postCommit() {
        // Method is called within DirectUpdateHandler2's commit method
        // from inside the commitLock critical section
        // First check has enough time passed since the last diff for it to be worth it to diff right now

        if (System.currentTimeMillis() - this.lastDiffEpoch < this.diffInterval) {
            log.debug("Not enough time has passed since last diff, with interval {} miliseconds. Skipping...",
                    this.diffInterval);
            return;
        }

        // This is approximate, so it could be that there isn't actually an available thread
        // and this still happens
        if ( this.executor.getActiveCount() >= DIFF_THREAD_LIMIT) {
            log.debug("No thread available to perform diff, consider reducing the diff interval currently set at {} ms. " +
                    "Skipping...", diffInterval);
            return;
        }

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

            //Check that there is at least a commit
            if (commits.isEmpty()) {
                log.debug("No commits were found in post commit, skipping...");
                return;
            }

            // Now iterate through the commits
            List<Long> sortedGenerationNums = new ArrayList<>();
            sortedGenerationNums.addAll(commits.keySet());
            Collections.sort(sortedGenerationNums);
            long newestCommitGen = -1, earlierCommitGen = -1;
            // Now that the commits are sorted, we want to reserve them
            // We also only care about the last 2
            // NOTE this method will save the commit points before passing them off to a thread pool to handle the
            // diffing and publishing, it is in that task where the commit points must be released
            DiffTask task = null;
            if (sortedGenerationNums.size() == 1 ) {
                newestCommitGen = sortedGenerationNums.get(0);
                core.getDeletionPolicy().saveCommitPoint(newestCommitGen);
                IndexCommit newestCommit = core.getDeletionPolicy().getCommitPoint(newestCommitGen);
                task = new DiffTask(null, newestCommit, earlierCommitGen,
                        newestCommitGen, shardId, collectionName, fieldsToDiff, core.getDeletionPolicy());
            } else {
                newestCommitGen = sortedGenerationNums.get(sortedGenerationNums.size() - 1);
                earlierCommitGen = sortedGenerationNums.get(sortedGenerationNums.size() - 2);

                // Save the commit points so that the commits won't be deleted by something else until we are done
                core.getDeletionPolicy().saveCommitPoint(newestCommitGen);
                core.getDeletionPolicy().saveCommitPoint(earlierCommitGen);

                IndexCommit newerCommit = core.getDeletionPolicy().getCommitPoint(newestCommitGen);
                IndexCommit earlierCommit = core.getDeletionPolicy().getCommitPoint(earlierCommitGen);
                task = new DiffTask(earlierCommit, newerCommit, earlierCommitGen,
                        newestCommitGen, shardId, collectionName, fieldsToDiff, core.getDeletionPolicy());
            }
            handleDiffFuture(this.executor.submit(task));
            this.lastDiffEpoch = System.currentTimeMillis();
        }
    }

    @Override
    public void postSoftCommit() {

    }

    @Override
    public void newSearcher(SolrIndexSearcher solrIndexSearcher, SolrIndexSearcher solrIndexSearcher1) {

    }

    /**
     * Method is provided with the future of what is returned from the task submitted to the thread pool
     * Override this method to process any output
     * @param result    The result of the concurrent task
     */
    protected void handleDiffFuture(Future<?> result) {}

    /**
     * Method registers a close hook to the solr core to shutdown the executor service
     */
    protected void registerCloseHook() {
        this.getCore().addCloseHook(new EventListenerCloseHook());
    }

    abstract protected void updateDocumentFrequency(Map<String, List<DocumentFrequencyUpdate>> updates);

}
