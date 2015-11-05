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

import com.bloomberg.news.fennec.common.DocumentFrequencyKafkaSerializer;
import com.bloomberg.news.fennec.common.DocumentFrequencyUpdate;
import com.bloomberg.news.fennec.common.FennecConstants;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * The Kafka Producer used by the com.bloomberg.news.fennec.solr.DocumentFrequencyUpdateEventListener
 */
public class DocumentFrequencyUpdateProducer {
    private static final Logger log = LoggerFactory.getLogger(DocumentFrequencyUpdateProducer.class);

    private Producer<String, String> kafkaProducer;

    /**
     * Constructor allowing a custom properties file
     * @param propertiesFile
     * @throws IOException
     */
    public DocumentFrequencyUpdateProducer(String propertiesFile) throws IOException {
        log.info(String.format("Initializing Kafka Producer with %s as properties", propertiesFile));
        Properties properties = new Properties();
        properties.load(new FileInputStream(propertiesFile));

        ProducerConfig config = new ProducerConfig(properties);
        this.kafkaProducer = new Producer<>(config);
        log.info("Producer successfully initialized");
    }

    /**
     * Constructor using the default configurations
     */
    public DocumentFrequencyUpdateProducer() {
        log.info("Initializing Kafka Producer with default properties");
        Properties props = new Properties();
        props.put("metadata.broker.list", "sundev9.dev.bloomberg.com:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "com.bloomberg.news.fennec.com.bloomberg.news.fennec.solr.UpdatePartitioner");
        props.put("request.required.acks", "1");

        // This will handle concurrent sending and compression for us.
        // 1 thread / broker
        props.put("producer.type", "async");

        ProducerConfig config = new ProducerConfig(props);
        this.kafkaProducer = new Producer<>(config);
    }

    /**
     * Publish the updates to the kafka broker
     * @param updateMap Map of fields -> list of updates for that field
     */
    public void updateDocumentFrequency(HashMap<String, List<DocumentFrequencyUpdate>> updateMap) {

        log.debug("Producing messages from map {} for fields {}", updateMap.toString(), updateMap.keySet());
        // We should do this part async so that we don't impact solr performance
        for (String fieldName : updateMap.keySet()) {
            List<DocumentFrequencyUpdate> updates = updateMap.get(fieldName);
            for (DocumentFrequencyUpdate update : updates) {
                KeyedMessage<String, String> updateData =
                        new KeyedMessage<>( update.collectionName , update.getKey(),
                                DocumentFrequencyKafkaSerializer.serialize(update));
                this.kafkaProducer.send(updateData);
            }
        }
    }

    /**
     * Method to invoke to shutdown extra kafka threads
     */
    public void shutdown() {
        this.kafkaProducer.close();
    }


}
