package org.apache.solr.analysis;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.LuceneTestCase;

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

public class TestRepeatingTokenFilter extends LuceneTestCase{
  
  public final void testRepeatingToken() throws IOException
  {
    String termValue = "oil";
    int frequency = 5;
    String delimiter = ":";
    StringReader reader = new StringReader(termValue + delimiter + frequency);
    Map<String,String> initParams = new HashMap<>();
    initParams.put(RepeatingTokenFilterFactory.DELIMITER_ATTR, delimiter);
    RepeatingTokenFilterFactory factory = new RepeatingTokenFilterFactory(initParams);
    TokenStream stream = new MockTokenizer(reader);
    stream = factory.create(stream);
    
    CharTermAttribute termAtt = null;
    assertTrue("has no CharTermAttribute", stream.hasAttribute(CharTermAttribute.class));
    termAtt = stream.getAttribute(CharTermAttribute.class);
    
    stream.reset();
    for (int i = 0; i < frequency; i++) {
      assertTrue("failed to increment token stream",stream.incrementToken());
      assertEquals("term "+i, termValue, termAtt.toString());
    }
    assertFalse(stream.incrementToken());
  }
  
}
