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
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Logic for performing an index diff between the latest and past commits.
 * The differ only produces a list of terms that have changed, and returns the new doc freq values not a difference in doc freq value
 */
public class DocumentFrequencyIndexDiffer {
    private static final Logger log = LoggerFactory.getLogger(DocumentFrequencyIndexDiffer.class);
    
    protected static void addMetadata(Map<String, List<DocumentFrequencyUpdate>> updates, 
                                    Map<String, Integer> docCounts, 
                                    String collection,
                                    String shard) {
        final int docCountsSize = docCounts.size();
        final int updatesSize = updates.size();
        if (docCountsSize != updatesSize) {
            log.warn("updates size={}, docCounts size={} should be equal", updatesSize, docCountsSize);
        }
        
        for (Map.Entry<String, Integer> docCount : docCounts.entrySet()) {
            for (DocumentFrequencyUpdate update : updates.get(docCount.getKey())) {
                update.setTotalNumDocs(docCount.getValue());
                update.setCollectionName(collection);
                update.setShard(shard);
            }
        }
    }

    /**
     * The point of entry, will check if this is the first entry and then delegate the diffing to the other private methods
     * @param earlierCommit     The baseline for the diff, can be null
     * @param laterCommit       The commit to diff against the baseline, can be null
     * @param fieldsFilter      Fields to include in the diff, if null we will diff all fields
     * @return                  A map of Fields -> list of updates for that field.
     *                          Returns null if the diff could not be performed.                          
     */
    public static Map<String, List<DocumentFrequencyUpdate>> diffCommits(IndexCommit earlierCommit,
                                                                         IndexCommit laterCommit, 
                                                                         Set<String> fieldsFilter,
                                                                         String collection,
                                                                         String shard) {
        final long startTime = System.nanoTime();
        
        final Map<String, List<DocumentFrequencyUpdate>> diff;
        final Map<String, Integer> docCounts;

        if (fieldsFilter != null) {
            diff = new HashMap<String, List<DocumentFrequencyUpdate>>(fieldsFilter.size());
            docCounts = new HashMap<String, Integer>(fieldsFilter.size());
        } else {
            diff = new HashMap<String, List<DocumentFrequencyUpdate>>();
            docCounts = new HashMap<String, Integer>();
        }        
        
        try (DirectoryReader earlierCommitSearcher = (earlierCommit == null ? null : DirectoryReader.open(earlierCommit));
             DirectoryReader laterCommitSearcher   = (laterCommit   == null ? null : DirectoryReader.open(laterCommit))) {

            final Fields earlierCommitFields = earlierCommitSearcher == null ? null : MultiFields.getFields(earlierCommitSearcher);
            final Fields laterCommitFields   = laterCommitSearcher   == null ? null : MultiFields.getFields(laterCommitSearcher);
            
            final Set<String> fields;
            if (fieldsFilter != null) {
                fields = fieldsFilter;
            } else {
                fields = new HashSet<String>();
                // unless there is an easier way of adding an iterable to a collection
                if (earlierCommitFields != null) {
                    final Iterator<String> earlierCommitFieldNames = earlierCommitFields.iterator();
                    while (earlierCommitFieldNames.hasNext()) {
                        fields.add(earlierCommitFieldNames.next());
                    }
                }
                
                if (laterCommitFields != null) {
                    final Iterator<String> laterCommitFieldNames = laterCommitFields.iterator();
                    while (laterCommitFieldNames.hasNext()) {
                        fields.add(laterCommitFieldNames.next());
                    }
                }
            }

            for (final String field : fields) {
                final Terms earlierCommitTerms = (earlierCommitFields == null ? null : earlierCommitFields.terms(field));
                final Terms laterCommitTerms = (laterCommitFields == null ? null : laterCommitFields.terms(field));
                
                if (earlierCommitTerms != null || laterCommitTerms != null) {
                    diff.put(field, diffTerms(field, earlierCommitTerms, laterCommitTerms));
                    docCounts.put(field, laterCommitSearcher.getDocCount(field));
                }
            }
        } catch (IOException e) {
            log.info("Exception caught whilst reading the commits: {}", e);
            return null;
        } catch (UnsupportedOperationException e) {
            log.info("Exception caught whilst diffing the terms: {}", e);
            return null;
        }
        
        addMetadata(diff, docCounts, collection, shard);
        
        log.info("Differ produced diffs for {} fields in {} milliseconds, earlierCommit={}, laterCommit={}", 
                  fieldsFilter == null ? "all" : fieldsFilter.size(),
                  TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS),
                  earlierCommit,
                  laterCommit);
        
        return diff;
    }

    /**
     * @param firstTerms The first terms to be compared, can be null.
     * @param secondTerms The second terms to be compared, can be null.
     * @return Returns a list of terms that have different frequencies in the first and second terms.
     * @throws IOException  Throws IOException if there is a problem reading the terms
     */
    private static List<DocumentFrequencyUpdate> diffTerms(String field, Terms firstTerms, Terms secondTerms) throws IOException {
        final TermsEnum firstTermsEnum = (firstTerms == null ? null : firstTerms.iterator(null));
        final TermsEnum secondTermsEnum = (secondTerms == null ? null : secondTerms.iterator(null));
        
        if (firstTermsEnum != null && secondTermsEnum != null 
              && ! firstTermsEnum.getComparator().equals(secondTermsEnum.getComparator())) {
            throw new UnsupportedOperationException("Cannot diff terms with different comparators, first=" 
                                                    + firstTermsEnum.getComparator() 
                                                    + ", second=" + secondTermsEnum.getComparator());
        }
        
        final Comparator<BytesRef> comparator = (firstTermsEnum != null ?
                                                    firstTermsEnum.getComparator() : 
                                                    secondTermsEnum.getComparator());
        
        BytesRef firstBytesRef = (firstTermsEnum == null ? null : firstTermsEnum.next());
        BytesRef secondBytesRef = (secondTermsEnum == null ? null : secondTermsEnum.next());
        
        final List<DocumentFrequencyUpdate> updatedTerms = new LinkedList<DocumentFrequencyUpdate>();
        
        while (firstBytesRef != null && secondBytesRef != null) {
            final int termComparison = comparator.compare(firstBytesRef, secondBytesRef);

            if (termComparison < 0) {
                // deleted term
                updatedTerms.add(new DocumentFrequencyUpdate(field, 
                                                             firstBytesRef.utf8ToString(), 
                                                             System.nanoTime(), 
                                                             0,
                                                             -firstTermsEnum.docFreq()));
                firstBytesRef = firstTermsEnum.next();
            } else if (termComparison > 0) {
                // new term
                updatedTerms.add(new DocumentFrequencyUpdate(field, 
                                                             secondBytesRef.utf8ToString(), 
                                                             System.nanoTime(), 
                                                             secondTermsEnum.docFreq(), 
                                                             secondTermsEnum.docFreq()));
                secondBytesRef = secondTermsEnum.next();
            } else {
                if (firstTermsEnum.docFreq() != secondTermsEnum.docFreq()) {
                    updatedTerms.add(new DocumentFrequencyUpdate(field, 
                                                                 firstBytesRef.utf8ToString(), 
                                                                 System.nanoTime(), 
                                                                 secondTermsEnum.docFreq(), 
                                                                 secondTermsEnum.docFreq() - firstTermsEnum.docFreq()));
                }

                firstBytesRef = firstTermsEnum.next();
                secondBytesRef = secondTermsEnum.next();
            }
        }
        
        while (firstBytesRef != null) {
            // deleted term
            updatedTerms.add(new DocumentFrequencyUpdate(field, 
                                                         firstBytesRef.utf8ToString(), 
                                                         System.nanoTime(), 
                                                         0,
                                                         -firstTermsEnum.docFreq()));
            firstBytesRef = firstTermsEnum.next();
        }
        
        while (secondBytesRef != null) {
            // new term
            updatedTerms.add(new DocumentFrequencyUpdate(field, 
                                                         secondBytesRef.utf8ToString(), 
                                                         System.nanoTime(), 
                                                         secondTermsEnum.docFreq(),
                                                         secondTermsEnum.docFreq()));
            secondBytesRef = secondTermsEnum.next();
        }
        
        return updatedTerms;
    }
}
