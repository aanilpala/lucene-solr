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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * An implementation of a AbstractDocumentFrequencyupdateEventListener that uses Kafka to publish the frequencies
 */
public class KafkaDocumentFrequencyUpdateEventListener extends AbstractDocumentFrequencyUpdateEventListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaDocumentFrequencyUpdateEventListener.class);

    // Differ doesn't store any state so the fields to diff on are here
    protected KafkaDocumentFrequencyUpdateProducer producer;

    /**
     * Constructor called during solr initialization
     * @param core
     * @throws IOException
     */
    public KafkaDocumentFrequencyUpdateEventListener(SolrCore core) throws IOException {
        super(core);
    }

    @Override
    public void init(NamedList args) {
        super.init(args);
        log.info("Initializing Kafka Event Listener");
        try {
            this.producer= new KafkaDocumentFrequencyUpdateProducer(args);
            log.info("Finished initializing kafka event listener");
        } catch (IOException e) {
            log.error("Unable to initialize kafka producer");
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to initialize Event Listener", e);
        }

    }

    @Override
    protected void updateDocumentFrequency(Map<String, List<DocumentFrequencyUpdate>> updates) {
        this.producer.updateDocumentFrequency(updates);

    }

}
