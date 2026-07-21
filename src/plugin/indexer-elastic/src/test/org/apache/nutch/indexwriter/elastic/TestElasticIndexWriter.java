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
package org.apache.nutch.indexwriter.elastic;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.nutch.indexer.NutchDocument;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestElasticIndexWriter {

  @Test
  void testCreateSourceMapPreservesCurrentFieldShape() {
    ElasticIndexWriter writer = new ElasticIndexWriter();
    NutchDocument doc = new NutchDocument();
    Date fetchTime = Date.from(Instant.parse("2024-01-02T03:04:05Z"));

    doc.add("id", "doc-1");
    doc.add("title", "Test Document");
    doc.add("tag", "one");
    doc.add("tag", "two");
    doc.add("fetchTime", fetchTime);

    Map<String, Object> source = writer.createSourceMap(doc);

    assertEquals("doc-1", source.get("id"));
    assertEquals("Test Document", source.get("title"));
    assertEquals(List.of("one", "two"), source.get("tag"));
    assertEquals("2024-01-02T03:04:05Z", source.get("fetchTime"));
  }

  @Test
  void testRetryableStatuses() {
    assertTrue(ElasticIndexWriter.isRetryableStatus(429));
    assertTrue(ElasticIndexWriter.isRetryableStatus(502));
    assertTrue(ElasticIndexWriter.isRetryableStatus(503));
    assertTrue(ElasticIndexWriter.isRetryableStatus(504));

    assertFalse(ElasticIndexWriter.isRetryableStatus(400));
    assertFalse(ElasticIndexWriter.isRetryableStatus(401));
    assertFalse(ElasticIndexWriter.isRetryableStatus(404));
    assertFalse(ElasticIndexWriter.isRetryableStatus(409));
  }

  @Test
  void testExponentialBackoffMillis() {
    assertEquals(100L,
        ElasticIndexWriter.computeExponentialBackoffMillis(100, 0));
    assertEquals(200L,
        ElasticIndexWriter.computeExponentialBackoffMillis(100, 1));
    assertEquals(400L,
        ElasticIndexWriter.computeExponentialBackoffMillis(100, 2));
    assertEquals(0L,
        ElasticIndexWriter.computeExponentialBackoffMillis(0, 2));
  }
}
