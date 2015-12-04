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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Logic for performing an index diff between the latest and past commits.
 * The differ only produces a list of terms that have changed, and returns the new doc freq values not a difference in doc freq value
 */
public class DocumentFrequencyIndexDiffer {
    private static final Logger log = LoggerFactory.getLogger(DocumentFrequencyIndexDiffer.class);

    // The new commit shows no records at all, and has no fields
    private static Map<String, List<DocumentFrequencyUpdate>> diffClearedIndex(IndexCommit newCommit, IndexCommit oldCommit,
                                                                                   long timestamp, String shardId,
                                                                                   String collectionName,
                                                                                   Set<String> fieldSet) throws IOException {
        HashMap<String, List<DocumentFrequencyUpdate>> deletionMap = new HashMap<>();
        DirectoryReader oldSearcher = DirectoryReader.open(oldCommit);
        for (String fieldName : MultiFields.getFields(oldSearcher)) {

            // Skip if fieldSet is specified and this is not in there
            if (fieldSet != null && !fieldSet.contains(fieldName)) continue;
            List<DocumentFrequencyUpdate> updateList = new ArrayList<>();

            TermsEnum oldTerms = null;
            oldTerms = MultiFields.getTerms(oldSearcher, fieldName).iterator(oldTerms);
            while (oldTerms.next() != null) {
                // Build the update here
                // Terms have 0 doc freq and there are 0 docs in the whole index
                DocumentFrequencyUpdate update = new DocumentFrequencyUpdate(fieldName, oldTerms.term().utf8ToString(),
                        collectionName, shardId, timestamp, 0L, 0);
                updateList.add(update);
            }
            deletionMap.put(fieldName, updateList);
        }
        oldSearcher.close();
        return deletionMap;
    }


    /**
     * Main diffing logic, for handling regular commits
     * First for fields appearing in the new commit, we check for new and updated terms
     * then look for terms that were deleted.
     * Then we look through fields that are in the old commit but not in the new commit
     * Because the fields diffed in the two loops are mutually exclusive,
     * all update lists inserted into the map are complete when they are inserted
     * @param newCommit         new commit that has a larger commit number
     * @param oldCommit         older commit
     * @param timestamp         timestamp associated when the diffing occurred
     * @param shardId           the shard this diff is for
     * @param collectionName    the collection this diff is for
     * @param fieldSet          the set of field names that we are interested in producing diffs for, if null, all fields
     * @return
     * @throws IOException
     */
    private static Map<String, List<DocumentFrequencyUpdate>> commitDiff (IndexCommit newCommit, IndexCommit oldCommit, long timestamp,
                                                                        String shardId, String collectionName, Set<String> fieldSet) throws IOException {

        log.debug("Diffing two commits new: {} and old: {}", newCommit, oldCommit);

        DirectoryReader newSearcher = DirectoryReader.open(newCommit);
        DirectoryReader oldSearcher = DirectoryReader.open(oldCommit);
        HashMap<String, List<DocumentFrequencyUpdate>> changeInDocFreq = new HashMap<>();

        // Add checks for edge cases like when the entire index is deleted
        if (MultiFields.getFields(newSearcher) == null || !MultiFields.getFields(newSearcher).iterator().hasNext()) {
            newSearcher.close();
            oldSearcher.close();
            return diffClearedIndex(newCommit, oldCommit, timestamp, shardId, collectionName, fieldSet);
        }

        // Important, empty commits still register as a commit
        // Hence, diff needs to check that the previous commit is not an empty commit on an empty index
        // because that would be the same logic as diffing the very first index commit
        if (oldSearcher.numDocs() == 0) {
            newSearcher.close();
            oldSearcher.close();
            return diffFirstCommit(newCommit, timestamp, shardId, collectionName, fieldSet);
        }

        int numDocs = newSearcher.numDocs();
        // In the case where the index was cleared, then re-added with less fields,
        // The doc frequency server we update will detect that a previously updated field has disappeared
        // and delete it there
        for (String fieldName : MultiFields.getFields(newSearcher)) {
            // Skip if fieldSet is specified and this is not in there
            if (fieldSet != null && !fieldSet.contains(fieldName)) continue;
            List<DocumentFrequencyUpdate> updateList = new ArrayList<>();
            Terms newTerms =  MultiFields.getTerms(newSearcher, fieldName);
            Terms oldTerms = MultiFields.getTerms(oldSearcher, fieldName);

            // This could happen if the previous index was cleared and
            // the new documents added don't have the field anymore
            // We will simply ignore this field
            if (oldTerms == null) continue;

            // The iterator that is returned, has next pointing to the first term
            TermsEnum newTermEnum = null;
            newTermEnum = newTerms.iterator(newTermEnum);
            TermsEnum oldTermEnum = null;
            oldTermEnum = oldTerms.iterator(oldTermEnum);

            // Iterate over the new terms and the old terms
            while (newTermEnum.next() != null) {
                int docFreq = newTermEnum.docFreq();

                // We need to capture deleted terms
                // inserted terms and updated terms

                boolean oldTermFound = oldTermEnum.seekExact(newTermEnum.term());
                // Old term changed or New term added
                if ( (oldTermFound && oldTermEnum.docFreq() != docFreq ) ||
                        !oldTermFound) {
                    // Store the change in docFreq
                    DocumentFrequencyUpdate update = new DocumentFrequencyUpdate(fieldName,
                            newTermEnum.term().utf8ToString(), collectionName, shardId, timestamp, newTermEnum.docFreq(),
                            numDocs);

                    updateList.add(update);
                }
            }

            //Now we iterate through this
            oldTermEnum = oldTerms.iterator(oldTermEnum);
            newTermEnum = newTerms.iterator(newTermEnum);

            // Look for deleted terms
            while (oldTermEnum.next() != null) {

                if (!newTermEnum.seekExact(oldTermEnum.term())) {
                    DocumentFrequencyUpdate update = new DocumentFrequencyUpdate(fieldName,
                            oldTermEnum.term().utf8ToString(), collectionName, shardId, timestamp, 0L,
                            numDocs);
                    updateList.add(update);
                }
            }

            // Now we have iterated over all the terms for this field
            // Add the update to the field
            changeInDocFreq.put(fieldName, updateList);
        }

        // Before we didn't handle the case of a field being in the old commit but no longer in the new commit
        // AND the old commit is not empty
        for (String fieldName : MultiFields.getFields(oldSearcher)) {
            // Skip if fieldSet is specified and this is not in there, or we have already diffed it because the field
            // also is in the new commit
            if ( (fieldSet != null && !fieldSet.contains(fieldName))
                    || changeInDocFreq.containsKey(fieldName)) continue;
            List<DocumentFrequencyUpdate> updateList = new ArrayList<>();
            Terms oldTerms = MultiFields.getTerms(oldSearcher, fieldName);

            // Now add all terms in this field as 0
            TermsEnum oldTermEnum = null;
            oldTermEnum = oldTerms.iterator(oldTermEnum);
            while (oldTermEnum.next() != null) {
                DocumentFrequencyUpdate update = new DocumentFrequencyUpdate(fieldName,
                        oldTermEnum.term().utf8ToString(), collectionName, shardId, timestamp, 0L,
                        numDocs);

                updateList.add(update);
            }

            changeInDocFreq.put(fieldName, updateList);
        }

        newSearcher.close();
        oldSearcher.close();
        return changeInDocFreq;
    }

    // Create updates for all terms present used for a commit against an empty index
    private static List<DocumentFrequencyUpdate> createUpdateList(TermsEnum termsIterator, String fieldName,
                                                                  long timestamp,
                                                                  String collectionName,
                                                                  String shardId,
                                                                  int totalNumDocs) throws IOException {
        List<DocumentFrequencyUpdate> updateList = new ArrayList<>();
        while (termsIterator.next() != null) {

            DocumentFrequencyUpdate update = new DocumentFrequencyUpdate(fieldName, termsIterator.term().utf8ToString(),
                    collectionName, shardId, timestamp, termsIterator.docFreq(), totalNumDocs);
            updateList.add(update);
        }
        return updateList;
    }

    // Used when there is only 1 commit in the commit list
    private static Map<String, List<DocumentFrequencyUpdate>> diffFirstCommit(IndexCommit commit, long timestamp,
                                                                                  String shardId,
                                                                                  String collectionName,
                                                                                  Set<String> fieldSet) throws IOException {

        log.debug("Producing update for first commit as flush {} at {}", commit.getGeneration(), timestamp);
        IndexReader indexSearcher = DirectoryReader.open(commit);
        HashMap<String, List<DocumentFrequencyUpdate>> changeInDocFreq = new HashMap<>();

        if (indexSearcher == null) {
            log.info("No index searcher opened to diff the first commit");
            return changeInDocFreq;
        }

        int numDocs = indexSearcher.numDocs();

        // Account for edge cases on empty collection
        Fields fields = MultiFields.getFields(indexSearcher);
        if (fields == null) {
            log.info("No fields found for index search");
            return changeInDocFreq;
        }

        for (String fieldName : fields) {
            // Skip if fieldSet is specified and this is not in there
            if (fieldSet != null && !fieldSet.contains(fieldName)) continue;

            Terms terms = MultiFields.getTerms(indexSearcher, fieldName);
            TermsEnum termsIterator = null;
            termsIterator = terms.iterator(termsIterator);

            List<DocumentFrequencyUpdate> updateList = createUpdateList(termsIterator, fieldName,
                    timestamp, collectionName, shardId, numDocs);
            changeInDocFreq.put(fieldName, updateList);
        }

        indexSearcher.close();
        return changeInDocFreq;
    }

    /**
     * The point of entry, will check if this is the first entry and then delegate the diffing to the other private methods
     * @param earlierCommit
     * @param laterCommit
     * @param shardId           The shard this diff is happening at
     * @param collectionName    The name of the collection
     * @param fieldSet          Fields to include in the diff, if null we will diff all fields
     * @return                  A map of Fields -> list of updates for that field
     * @throws IOException
     */
    public static Map<String, List<DocumentFrequencyUpdate>> diffCommits(IndexCommit earlierCommit,
                                                                             IndexCommit laterCommit, String shardId,
                                                                             String collectionName,
                                                                             Set<String> fieldSet) throws IOException {
        long timestamp = System.currentTimeMillis();

        Map<String , List<DocumentFrequencyUpdate>> changeToPost;
        // We get the commit data here
        if (earlierCommit== null && laterCommit != null) {
            changeToPost = diffFirstCommit(laterCommit, timestamp, shardId, collectionName, fieldSet);
        } else if (earlierCommit != null && laterCommit != null) {
            changeToPost = commitDiff(laterCommit, earlierCommit, timestamp, shardId, collectionName, fieldSet);
        } else {
            changeToPost = (HashMap<String, List<DocumentFrequencyUpdate>>) Collections.<String, List<DocumentFrequencyUpdate>>emptyMap();
        }

        log.debug("Differ produced diffs for {} fields stored completed in {} miliseconds", fieldSet == null ? "all" : fieldSet.size()
                , System.currentTimeMillis() - timestamp);
        return changeToPost;
    }

}
