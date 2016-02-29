package com.bloomberg.news.fennec.solr.rabbitmq;

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
import com.bloomberg.news.fennec.solr.AbstractDocumentFrequencyUpdateEventListener;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.CloseHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

/**
 * An implementation of a AbstractDocumentFrequencyupdateEventListener that uses RabbitMQ to publish the frequencies
 */
public class RabbitMQDocumentFrequencyUpdateEventListener extends AbstractDocumentFrequencyUpdateEventListener {
    private static final Logger log = LoggerFactory.getLogger(RabbitMQDocumentFrequencyUpdateEventListener.class);
    
    private static final String VHOST_CONFIG = "fennec.rabbitmq.vhost";
    private static final String HOST_CONFIG = "fennec.rabbitmq.host";
    private static final String PORT_CONFIG = "fennec.rabbitmq.port";
    private static final String USERNAME_CONFIG = "fennec.rabbitmq.username";
    private static final String PASSWORD_CONFIG = "fennec.rabbitmq.password";
    private static final String EXCHANGE_NAME_CONFIG = "fennec.rabbitmq.exchange";
    private static final String EXCHANGE_TYPE_CONFIG = "fennec.rabbitmq.exchangeType";

    private String exchangeName;

    private Connection connection;
    private Channel channel;
    
    /**
     * Constructor called during solr initialization
     * @param core The core that contains the commits to diff
     */
    public RabbitMQDocumentFrequencyUpdateEventListener(SolrCore core) {
        super(core);
        
        if (core.getCoreDescriptor().getCloudDescriptor() == null) {
            log.error("RabbitMQ producer requires cloud setup");
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "RabbitMQ producer requires cloud setup");
        }
    }

    @Override
    public void init(NamedList args) {
        super.init(args);
        log.info("Initializing RabbitMQ listener");
        
        boolean missingConfig = false;

        final String vhost = (String) args.get(VHOST_CONFIG);
        if (vhost == null) {
            log.error("Virtual Host is missing from config");
            missingConfig = true;
        }
        
        final String host = (String) args.get(HOST_CONFIG);
        if (host == null) {
            log.error("Host is missing from config");
            missingConfig = true;
        }
        
        final Integer port = (Integer) args.get(PORT_CONFIG);
        if (port == null) {
            log.error("Port is missing from config");
            missingConfig = true;
        }
        
        final String username = (String) args.get(USERNAME_CONFIG);
        if (username == null) {
            log.error("Username is missing from config");
            missingConfig = true;
        }
        
        final String password = (String) args.get(PASSWORD_CONFIG);
        if (password == null) {
            log.error("Password is missing from config");
            missingConfig = true;
        }
        
        exchangeName = (String) args.get(EXCHANGE_NAME_CONFIG);
        if (exchangeName == null) {
            log.error("Exchange name is missing from config");
            missingConfig = true;
        }
        
        final String exchangeType = (String) args.get(EXCHANGE_TYPE_CONFIG);
        if (exchangeType == null) {
            log.error("Exchange type is missing from config");
            missingConfig = true;
        }
        
        if (missingConfig) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Required RabbitMQ exchange type is missing from solrconfig.xml");
        }
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setVirtualHost(vhost);
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
        
            channel.exchangeDeclare(exchangeName, exchangeType);
        } catch (IOException|TimeoutException e) {
            log.error("Unable to establish RabbitMQ connection", e);
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to establish RabbitMQ connection", e);
        }

        registerCloseHook(new RabbitMQCloseHook(channel, connection));

        log.info("Finished initializing RabbitMQ listener");
    }

    @Override
    protected void updateDocumentFrequency(Map<String, List<DocumentFrequencyUpdate>> updateMap) {
        final long startTime = System.nanoTime();
        
        final int maxErrorsToPrint = 10;

        int successfulUpdates = 0;
        int failedUpdates = 0;

        for(Map.Entry<String, List<DocumentFrequencyUpdate>> entry :  updateMap.entrySet()) {
            final String fieldName = entry.getKey();
            final List<DocumentFrequencyUpdate> updates = entry.getValue();

            for (DocumentFrequencyUpdate update : updates) {
                try {
                    channel.basicPublish(exchangeName, collectionName /* routingKey */, null /* props */, update.serialize().getBytes());
                    ++successfulUpdates;
                } catch (IOException e) {
                    ++failedUpdates;
                    final String errorMessage = "Unable to send update: {}";
                    if (failedUpdates < maxErrorsToPrint) {
                        log.info(errorMessage, update, e);
                    } else {
                        log.trace(errorMessage, update, e);
                    }
                }
            }
        }
        
        if (failedUpdates > 0) {
            log.error("Failed to send {} updates to RabbitMQ", failedUpdates);
        }
        
        log.info("Publishing to RabbitMQ took {} ms, successfulUpdates={} failedUpdates={}", 
                 TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS), 
                 successfulUpdates, 
                 failedUpdates);
        
    }

    protected class RabbitMQCloseHook extends CloseHook {
        private final Channel channel;
        private final Connection connection;

        public RabbitMQCloseHook(final Channel channel, final Connection connection) {
            this.channel = channel;
            this.connection = connection;
        }

        @Override
        public void preClose(SolrCore core) {
            try {
                channel.close();
                connection.close();
            } catch (IOException|TimeoutException e) {
                log.error("Unable to close RabbitMQ channel/connection", e);
            }
        }

        @Override
        public void postClose(SolrCore core) {
        }
    }
}
