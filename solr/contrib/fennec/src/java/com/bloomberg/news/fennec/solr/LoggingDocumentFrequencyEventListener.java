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
import org.apache.solr.core.SolrCore;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * DocumentFrequency Event listener that does not publish but logs all the updates
 */
public class LoggingDocumentFrequencyEventListener extends AbstractDocumentFrequencyUpdateEventListener {

    private static final Logger log = LoggerFactory.getLogger(LoggingDocumentFrequencyEventListener.class);

    public LoggingDocumentFrequencyEventListener(SolrCore core) {
        super(core);
    }

    @Override
    protected void updateDocumentFrequency(Map<String, List<DocumentFrequencyUpdate>> updateMap) {
        int recordCount = 0;
        JSONObject outputJson = new JSONObject();
        List<JSONObject> fields = new ArrayList<>();
        for (String fieldName : updateMap.keySet()) {
            List<DocumentFrequencyUpdate> updates =  updateMap.get(fieldName);
            recordCount += updates.size();
            JSONObject fieldUpdate = new JSONObject();
            fieldUpdate.put("size", updates.size());
            fieldUpdate.put("field", fieldName);
            if (updates.size() > 0) {
                Collections.shuffle(updates);
                DocumentFrequencyUpdate update = updates.get(0);
                String updateData = DocumentFrequencyKafkaSerializer.serialize(update);
                fieldUpdate.put("sample", updateData);
            }
            fields.add(fieldUpdate);
        }
        outputJson.put("updates", fields);
        outputJson.put("total", recordCount);
        log.info("Publishing Updates: {}", outputJson.toJSONString());
    }
}
