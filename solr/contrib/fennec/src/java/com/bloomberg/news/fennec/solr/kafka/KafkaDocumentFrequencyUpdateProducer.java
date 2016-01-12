package com.bloomberg.news.fennec.solr.kafka;

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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.KafkaException;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * The Kafka Producer used by the com.bloomberg.news.fennec.solr.KafkaDocumentFrequencyUpdateEventListener
 */
public class KafkaDocumentFrequencyUpdateProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaDocumentFrequencyUpdateProducer.class);
    private static final String[] PRODUCER_CONFIGS = {"bootstrap.servers",
      "acks", "metadata.fetch.timeout.ms", "reconnect.backoff.ms",
      "retry.backoff.ms", "block.on.buffer.full"};

    private final KafkaProducer<String, String> kafkaProducer;

    public KafkaDocumentFrequencyUpdateProducer(NamedList args) {
        log.info("Initializing Kafka Producer");
        final Map<String, Object> properties = new HashMap<String, Object>(PRODUCER_CONFIGS.length, 1.0f);

        for (String key : PRODUCER_CONFIGS) {
            final Object value = args.get(key);
            if (value == null) {
                log.error("Required kafka config {} is missing", key);
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Required kafka key " + key + " is missing from solrconfig.xml");
            }

            properties.put(key, value);
        }

        log.info("Kafka producer properties read in: {}", properties);
        this.kafkaProducer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
        log.info("Producer successfully initialized");
    }

    /**
     * Publish the updates to the Kafka broker.
     * This happens in the background thread which computes the diff
     * and any delay here will delay the next diff.
     * @param updateMap Map of fields -> list of updates for that field
     */
    public void updateDocumentFrequency(Map<String, List<DocumentFrequencyUpdate>> updateMap) {
        final long startTime = System.nanoTime();
        log.debug("Producing messages from map {} for fields {}", updateMap.toString(), updateMap.keySet());
        
        final int maxErrorsToPrint = 10;
        int successfulUpdates = 0;
        int failedUpdates = 0;
        for(Map.Entry<String, List<DocumentFrequencyUpdate>> entry :  updateMap.entrySet()) {
            final String fieldName = entry.getKey();
            final List<DocumentFrequencyUpdate> updates = entry.getValue();
            for (DocumentFrequencyUpdate update : updates) {
                if (update.getCollectionName() == null) {
                    final String errorMessage = "Cannot send update with null collectionName: {}";
                    if (failedUpdates < maxErrorsToPrint) {
                        log.info(errorMessage, update);
                    } else {
                        log.trace(errorMessage, update);
                    }
                    ++failedUpdates;
                }
                
                final ProducerRecord<String, String> updateData = new ProducerRecord<>(update.getCollectionName(), update.getKey(), update.serialize());
                
                try {
                    this.kafkaProducer.send(updateData);
                    ++successfulUpdates;
                } catch (KafkaException e) {
                    final String errorMessage = "Unable to send update: {}";
                    if (failedUpdates < maxErrorsToPrint) {
                        log.info(errorMessage, updateData, e);
                    } else {
                        log.trace(errorMessage, updateData, e);
                    }
                    ++failedUpdates;
                }
            }
        }
        
        if (failedUpdates > 0) {
            log.error("Failed to send {} updates to Kafka", failedUpdates);
        }
        
        log.info("Publishing to Kafka took {} ms, successfulUpdates={} failedUpdates={}", 
                 TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS), 
                 successfulUpdates, 
                 failedUpdates);
    }

    /**
     * Method to invoke to shutdown extra kafka threads
     */
    public void shutdown() {
        this.kafkaProducer.close();
    }

}
