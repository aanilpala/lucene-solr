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

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/**
 * DocumentFrequency Event listener that does not publish but logs all the updates
 */
public class LoggingDocumentFrequencyEventListener extends AbstractDocumentFrequencyUpdateEventListener {
  
    enum SampleStrategy {
        INORDER {
            @Override
            public List<DocumentFrequencyUpdate> sample(List<DocumentFrequencyUpdate> updates, Integer sampleSize) {
                if (sampleSize == null) {
                    return updates;
                }
                
                return updates.subList(0, Math.min(sampleSize, updates.size()));
            }
            
            @Override
            public String toString() {
                return "In-Order";
            }
        },
        
        RANDOM {
            @Override
            public List<DocumentFrequencyUpdate> sample(List<DocumentFrequencyUpdate> updates, Integer sampleSize) {
                Collections.shuffle(updates); // O(updates.size())
                
                if (sampleSize == null) {
                    return updates;
                }
                
                return updates.subList(0, Math.min(sampleSize, updates.size()));
            }
            
            @Override
            public String toString() {
                return "Random";
            }
        };
        
        abstract public List<DocumentFrequencyUpdate> sample(List<DocumentFrequencyUpdate> updates, Integer sampleSize);
        abstract public String toString();
    };

    private static final Logger log = LoggerFactory.getLogger(LoggingDocumentFrequencyEventListener.class);
    private static final String SAMPLESIZE_ARG = "sample.size";
    private static final String SAMPLESTRATEGY_ARG = "sample.strategy";
    
    Integer sampleSize; // null sampleSize is full sample
    SampleStrategy sampleStrategy = SampleStrategy.INORDER;

    public LoggingDocumentFrequencyEventListener(SolrCore core) {
        super(core);
    }
    
    @Override
    public void init(NamedList args) {
        super.init(args);
        this.sampleSize = (Integer) args.get(SAMPLESIZE_ARG);
        try {
            this.sampleStrategy = SampleStrategy.valueOf((String) args.get(SAMPLESTRATEGY_ARG));
        } catch (Exception e) {
            log.debug("No/invalid sample strategy specified, falling back to default");
        }
        
        log.info("Sample size={}, strategy={}", sampleSize, sampleStrategy);
    }

    @Override
    protected void updateDocumentFrequency(Map<String, List<DocumentFrequencyUpdate>> updateMap) {
        final long startTime = System.nanoTime();
        
        List<Map<String, Object>> fields = new LinkedList<Map<String, Object>>();
        
        int totalTermsChanged = 0;

        for (String fieldName : updateMap.keySet()) {
            List<DocumentFrequencyUpdate> updates = updateMap.get(fieldName);
            totalTermsChanged += updates.size();

            Map<String, Object> fieldData = new HashMap<String, Object>(3);
            fieldData.put("termsChanged", updates.size());
            fieldData.put("field", fieldName);

            if (! updates.isEmpty()) {
                List<DocumentFrequencyUpdate> sample = sampleStrategy.sample(updates, sampleSize);
                fieldData.put("sample", sample);
            }
            
            fields.add(fieldData);
        }
        
        Map<String, Object> data = new HashMap<String, Object>(2, 1.0f);
        data.put("fields", fields);
        data.put("totalTermsChanged", totalTermsChanged);
        
        try {
            ObjectMapper mapper = new ObjectMapper();
            log.info("Publishing Updates: {}", mapper.writeValueAsString(data));
        } catch (JsonProcessingException e) {
            log.error("Failed to log update: {}", e);
        }
        
        log.info("updateDocumentFrequency completed in {} miliseconds", 
                 TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS));
    }
}
