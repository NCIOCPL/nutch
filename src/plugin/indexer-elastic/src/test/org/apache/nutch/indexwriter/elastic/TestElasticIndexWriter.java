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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

  @Test
  void testBulkListenerTracksActiveCallback() {
    ElasticIndexWriter writer = new ElasticIndexWriter();
    var listener = writer.bulkListener();

    listener.beforeBulk(1L, null, List.of());
    assertEquals(1, writer.activeBulkCallbacks());

    listener.afterBulk(1L, null, List.of(), successfulBulkResponse());
    assertEquals(0, writer.activeBulkCallbacks());
  }

  @Test
  void testBulkDrainReportsTimeout() throws Exception {
    ElasticIndexWriter writer = new ElasticIndexWriter();
    writer.open(testWriterParams());
    var listener = writer.bulkListener();

    listener.beforeBulk(1L, null, List.of());
    assertFalse(writer.awaitBulkCompletion(0, TimeUnit.MILLISECONDS));

    listener.afterBulk(1L, null, List.of(), successfulBulkResponse());
    writer.close();
  }

  @Test
  void testOpenStartsNewLifecycle() throws Exception {
    ElasticIndexWriter writer = new ElasticIndexWriter();
    writer.open(testWriterParams());
    writer.close();
    assertTrue(writer.isClosing());

    writer.open(testWriterParams());
    assertFalse(writer.isClosing());
    assertEquals(0, writer.activeBulkCallbacks());
    writer.close();
  }

  @Test
  void testOpenCleansUpResourcesWhenBulkIngesterCreationFails() {
    TrackingElasticIndexWriter writer = new TrackingElasticIndexWriter();
    IndexWriterParams invalidParams = new IndexWriterParams(Map.of(
        ElasticConstants.HOSTS, "localhost",
        ElasticConstants.PORT, "9200",
        ElasticConstants.MAX_BULK_DOCS, "-2"));

    assertThrows(IllegalArgumentException.class,
        () -> writer.open(invalidParams));

    assertFalse(writer.restClient.isRunning());
    assertTrue(writer.retryScheduler.isShutdown());
  }

  @Test
  void testQueuedRetryCannotAddOperationAfterCloseTimesOut() throws Exception {
    CapturingElasticIndexWriter writer = new CapturingElasticIndexWriter();
    IndexWriterParams params = new IndexWriterParams(Map.of(
        ElasticConstants.HOSTS, "localhost",
        ElasticConstants.PORT, "9200",
        ElasticConstants.EXPONENTIAL_BACKOFF_MILLIS, "60000",
        ElasticConstants.BULK_CLOSE_TIMEOUT, "0"));
    writer.open(params);

    var listener = writer.bulkListener();
    var context = new ElasticIndexWriter.RetryContext(
        writer.buildDeleteOperation("doc-1"), "delete document doc-1", 0);
    listener.beforeBulk(1L, null, List.of(context));
    listener.afterBulk(1L, null, List.of(context),
        new RuntimeException("test failure"));

    assertNotNull(writer.retryScheduler.scheduledCommand);
    writer.close();
    writer.retryScheduler.scheduledCommand.run();

    assertTrue(writer.isClosing());
    assertTrue(writer.awaitBulkCompletion(0, TimeUnit.MILLISECONDS));
  }

  private static BulkResponse successfulBulkResponse() {
    return BulkResponse.of(builder -> builder
        .errors(false)
        .items(List.of())
        .took(0));
  }

  private static IndexWriterParams testWriterParams() {
    return new IndexWriterParams(Map.of(
        ElasticConstants.HOSTS, "localhost",
        ElasticConstants.PORT, "9200"));
  }

  private static final class TrackingElasticIndexWriter
      extends ElasticIndexWriter {
    private RestClient restClient;
    private ScheduledExecutorService retryScheduler;

    @Override
    protected RestClient makeRestClient(IndexWriterParams parameters)
        throws java.io.IOException {
      restClient = super.makeRestClient(parameters);
      return restClient;
    }

    @Override
    ScheduledExecutorService createRetryScheduler() {
      retryScheduler = super.createRetryScheduler();
      return retryScheduler;
    }
  }

  private static final class CapturingElasticIndexWriter
      extends ElasticIndexWriter {
    private CapturingScheduledExecutor retryScheduler;

    @Override
    ScheduledExecutorService createRetryScheduler() {
      retryScheduler = new CapturingScheduledExecutor();
      return retryScheduler;
    }
  }

  private static final class CapturingScheduledExecutor
      extends ScheduledThreadPoolExecutor {
    private Runnable scheduledCommand;

    CapturingScheduledExecutor() {
      super(1);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay,
        TimeUnit unit) {
      scheduledCommand = command;
      return super.schedule(command, delay, unit);
    }
  }
}
