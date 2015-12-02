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

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;

import java.io.IOException;
import java.util.List;

/**
 * DeletionPolicy that will keep the 2 latest commits
 */
public class DocumentFrequencyUpdateDeletionPolicy extends IndexDeletionPolicy {

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        // Guaranteed that the list is sorted by age, 0th is the oldest commit
        // on start up, we want to publish full diff so mark all but the last commit as deleted
        // We want to publish a full diff initially because the doc frequency cache may be empty when we start up
        // and to optimize future updates, the differ will only publish updates of terms that changed between 2 commits
        // So to seed the doc frequency store/cache we need to have a full diff on startup
        for (int i = 0; i < commits.size() -1; i++) {
            commits.get(i).delete();
        }

        onCommit(commits);
    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        int size = commits.size();
        // We keep around 2 indices instead of just one
        for (int i = 0; i < size -2; i++) {
          commits.get(i).delete();
        }
      }

}
