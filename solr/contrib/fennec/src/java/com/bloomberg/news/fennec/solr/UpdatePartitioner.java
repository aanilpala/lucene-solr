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

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Partitioner used by the Kafka Producer to guarantee updates for the same (term field) pairs are sent
 * to the same partition for the topic
 */
public class UpdatePartitioner implements Partitioner<String>{

    public UpdatePartitioner(VerifiableProperties properties) {}

    @Override
    public int partition(String key, int i) {
        return (key.hashCode() % i);
    }

}