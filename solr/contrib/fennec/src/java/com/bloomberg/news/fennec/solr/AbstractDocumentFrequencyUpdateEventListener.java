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
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.AbstractSolrEventListener;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrDeletionPolicy;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
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
    private static final int DEFAULT_DIFF_INTERVAL_MS = 30000;
    private static final int DEFAULT_SHUTDOWN_TIME_SECONDS = 1;

    private static final String DIFF_INTERVAL_MS = "diff.interval.ms";

    // Differ doesn't store any state so the fields to diff on are here
    protected Set<String> fieldsToDiff;
    protected int diffIntervalMs = DEFAULT_DIFF_INTERVAL_MS;

    protected ThreadPoolExecutor executor;
    
    private final CloudDescriptor cloudDescriptor;
    private final String collectionName;
    private final String shardId;
    
    private Long previousDiffTime; // null previousDiffTime means no diff yet
    private Long previousCommitGeneration;

    /**
     * The task that contains the logic for performing the diff and also publishing
     * Should be submitted to the thread pool.
     */
    protected class DiffTask implements Runnable {
        private IndexCommit olderCommit;
        private IndexCommit newerCommit;

        public DiffTask(IndexCommit newerCommit) {
            this.newerCommit = newerCommit;            
        }

        @Override
        public void run() {
            final long startTime = System.nanoTime();
            boolean successfulDiff = false;
            
            try {
                if (previousCommitGeneration != null) {
                    olderCommit = getCore().getDeletionPolicy().getCommitPoint(previousCommitGeneration);
                    if (olderCommit == null) {
                        log.warn("Could not retreive older commit {}", previousCommitGeneration);
                    }
                }

                final Map<String, List<DocumentFrequencyUpdate>> updates = diffCommits(olderCommit, newerCommit);
                if (updates == null) {
                    // no diff would be empty map
                    log.warn("Diff was not successful");
                    return;
                }

                setPreviousDiffTime(startTime);
                updateDocumentFrequency(updates);

                previousCommitGeneration = newerCommit.getGeneration();
                successfulDiff = true;
            } finally {
                final IndexDeletionPolicyWrapper deletionPolicy = getCore().getDeletionPolicy();
                if (olderCommit != null) {
                    deletionPolicy.releaseCommitPoint(olderCommit.getGeneration());
                }
                
                if (! successfulDiff) {
                    // reset previous commit gen to null so that we don't keep trying to retrieve it
                    previousCommitGeneration = null;
                    // unsuccessful diff means previousCommitGeneration wouldn't have been updated to point to newerCommit,
                    // therefore we must release it here
                    deletionPolicy.releaseCommitPoint(newerCommit.getGeneration());
                }
            }
            
            log.info("Diff task completed in {} miliseconds", 
                     TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS));
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
                try {
                    executor.awaitTermination(DEFAULT_SHUTDOWN_TIME_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    log.warn("Executor failed to shutdown in {} seconds, forcing shutdown" , DEFAULT_SHUTDOWN_TIME_SECONDS);
                    executor.shutdownNow();
                }
            }
        }
    }

    public AbstractDocumentFrequencyUpdateEventListener(SolrCore core) {
        super(core);
        // we only want a single thread diffing and sending out updates
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);

        // We need to add a close hook to Solr because we need to shutdown our executor service
        registerCloseHook();
       
        cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
        
        if (cloudDescriptor == null) {
            shardId = null;
            collectionName = null;
        } else {
            shardId = cloudDescriptor.getShardId();
            collectionName = cloudDescriptor.getCollectionName();
        }
    }

    @Override
    public void init(NamedList args) {
        log.info("Initializing Abstract Event Listener, args={}", args);
        
        final Integer diffInterval = (Integer) args.get(DIFF_INTERVAL_MS);
        if (diffInterval != null) {
            this.diffIntervalMs = diffInterval.intValue();
        }
        log.info("Event listener configured to wait at least {} ms between diffs", this.diffIntervalMs);

        final String fields = (String) args.get(FennecConstants.FIELDS_KEY);
        if (fields != null) {
            this.fieldsToDiff = new HashSet<>(Arrays.asList(fields.split(FennecConstants.SEPARATOR)));
            log.info("Event listener configured to diff fields: {}", fieldsToDiff);
        } else {
            log.info("Event listener configured to diff all fields");
        }
        
        final IndexDeletionPolicy deletionPolicy = this.getCore().getDeletionPolicy().getWrappedDeletionPolicy();
        if (deletionPolicy instanceof SolrDeletionPolicy) {
            final int maxCommitsToKeep = ((SolrDeletionPolicy) deletionPolicy).getMaxCommitsToKeep();    
            if (maxCommitsToKeep < 2) {
                log.warn("SolrDeletionPolicy is keeping {} commits which is unusual", maxCommitsToKeep);
            }
        }

        log.info("Finished initializing abstract event listener");
    }

    @Override
    public void postCommit() {
        // Method is called within DirectUpdateHandler2's commit method
        // from inside the commitLock critical section
        // First check has enough time passed since the last diff for it to be worth it to diff right now
      
        final long startTime = System.nanoTime();

        final Long previousDiffTime = getPreviousDiffTime();
        if (previousDiffTime != null) {
            final Long interval = TimeUnit.MILLISECONDS.convert(System.nanoTime() - previousDiffTime, TimeUnit.NANOSECONDS);
            if (interval < this.diffIntervalMs) {
                log.debug("{} ms has passed since last diff, with interval {} miliseconds. Skipping...",
                          interval, this.diffIntervalMs);
                return;
            }
        }

        // If we are not a solrcloud node
        // or that we are, AND we are the leader
        // diff the indices and produce logs for kafka
        if ( cloudDescriptor != null && !cloudDescriptor.isLeader()) {
            log.debug("Not a leader in cloud mode, skipping");
            return;
        }

        final IndexDeletionPolicyWrapper deletionPolicy = getCore().getDeletionPolicy();
        final Long latestGenerationNum = deletionPolicy.getLatestCommit().getGeneration();
        deletionPolicy.saveCommitPoint(latestGenerationNum);
        final IndexCommit latestCommitPoint = deletionPolicy.getCommitPoint(latestGenerationNum);

        if (latestCommitPoint == null) {
            deletionPolicy.releaseCommitPoint(latestGenerationNum);
            log.debug("commit gen {} not available, skipping", latestGenerationNum);
            return;
        }
        
        final DiffTask diffTask = new DiffTask(latestCommitPoint);
        
        try {
            handleDiffFuture(this.executor.submit(diffTask));
        } catch (RejectedExecutionException e) {
            log.warn("Difftask was rejected: {}", e);
            deletionPolicy.releaseCommitPoint(latestGenerationNum);
            return;
        }
        
        log.info("postCommit completed in {} miliseconds", 
                 TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS));
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
    
    synchronized Long getPreviousDiffTime() {
        return previousDiffTime;
    }
    
    synchronized void setPreviousDiffTime(Long diffTime) {
        previousDiffTime = diffTime;
    }
    
    abstract protected void updateDocumentFrequency(Map<String, List<DocumentFrequencyUpdate>> updates);
    
    protected Map<String, List<DocumentFrequencyUpdate>> diffCommits(IndexCommit olderCommit, IndexCommit newerCommit) {
        return DocumentFrequencyIndexDiffer.diffCommits(olderCommit, newerCommit, fieldsToDiff, collectionName, shardId);
    }

}
