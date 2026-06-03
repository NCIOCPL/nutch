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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.nutch.util.StringUtil;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends NutchDocuments to a configured Elasticsearch index.
 */
public class ElasticIndexWriter implements IndexWriter {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final int DEFAULT_PORT = 9200;
  private static final int DEFAULT_MAX_BULK_DOCS = 250;
  private static final int DEFAULT_MAX_BULK_LENGTH = 2500500;
  private static final int DEFAULT_EXP_BACKOFF_MILLIS = 100;
  private static final int DEFAULT_EXP_BACKOFF_RETRIES = 10;
  private static final int DEFAULT_BULK_CLOSE_TIMEOUT = 600;
  private static final String DEFAULT_INDEX = "nutch";
  private static final String DEFAULT_USER = "elastic";
  private static final int MAX_CONCURRENT_BULK_REQUESTS = 1;
  private static final int DRAIN_POLL_MILLIS = 100;

  private String[] hosts;
  private int port;
  private String scheme = HttpHost.DEFAULT_SCHEME_NAME;
  private String user = null;
  private String password = null;
  private boolean auth;

  private int maxBulkDocs;
  private int maxBulkLength;
  private int expBackoffMillis;
  private int expBackoffRetries;

  private String defaultIndex;
  private ElasticsearchClient client;
  private ElasticsearchTransport transport;
  private BulkIngester<RetryContext> bulkIngester;
  private ScheduledExecutorService retryScheduler;
  private final AtomicInteger scheduledRetries = new AtomicInteger();
  private volatile boolean closing;

  private long bulkCloseTimeout;

  private Configuration config;

  @Override
  public void open(Configuration conf, String name) throws IOException {
    // Implementation not required
  }

  /**
   * Initializes the internal variables from a given index writer configuration.
   *
   * @param parameters
   *          Params from the index writer configuration.
   * @throws IOException
   *           Some exception thrown by writer.
   */
  @Override
  public void open(IndexWriterParams parameters) throws IOException {

    String hosts = parameters.get(ElasticConstants.HOSTS);

    if (StringUtils.isBlank(hosts)) {
      String message = "Missing elastic.host this should be set in index-writers.xml ";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    bulkCloseTimeout = parameters.getLong(ElasticConstants.BULK_CLOSE_TIMEOUT,
        DEFAULT_BULK_CLOSE_TIMEOUT);
    defaultIndex = parameters.get(ElasticConstants.INDEX, DEFAULT_INDEX);

    maxBulkDocs = parameters.getInt(ElasticConstants.MAX_BULK_DOCS,
        DEFAULT_MAX_BULK_DOCS);
    maxBulkLength = parameters.getInt(ElasticConstants.MAX_BULK_LENGTH,
        DEFAULT_MAX_BULK_LENGTH);
    expBackoffMillis = parameters.getInt(
        ElasticConstants.EXPONENTIAL_BACKOFF_MILLIS,
        DEFAULT_EXP_BACKOFF_MILLIS);
    expBackoffRetries = parameters.getInt(
        ElasticConstants.EXPONENTIAL_BACKOFF_RETRIES,
        DEFAULT_EXP_BACKOFF_RETRIES);

    client = makeClient(parameters);
    retryScheduler = Executors.newScheduledThreadPool(2,
        new ElasticBulkThreadFactory());

    LOG.debug("Creating BulkIngester with maxBulkDocs={}, maxBulkLength={}",
        maxBulkDocs, maxBulkLength);
    bulkIngester = BulkIngester.of(b -> b.client(client)
        .maxOperations(maxBulkDocs)
        .maxSize(maxBulkLength)
        .maxConcurrentRequests(MAX_CONCURRENT_BULK_REQUESTS)
        .scheduler(retryScheduler)
        .listener(bulkListener()));
  }

  /**
   * Generates an ElasticsearchClient with the hosts given.
   *
   * @param parameters implementation specific {@link IndexWriterParams}
   * @return an initialized {@link ElasticsearchClient}
   * @throws IOException if there is an error reading the {@link IndexWriterParams}
   */
  protected ElasticsearchClient makeClient(IndexWriterParams parameters)
      throws IOException {
    RestClient restClient = makeRestClient(parameters);
    transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
    return new ElasticsearchClient(transport);
  }

  /**
   * Generates a low-level RestClient with the hosts given.
   *
   * @param parameters implementation specific {@link IndexWriterParams}
   * @return an initialized {@link RestClient}
   * @throws IOException if there is an error reading the {@link IndexWriterParams}
   */
  protected RestClient makeRestClient(IndexWriterParams parameters)
      throws IOException {
    hosts = parameters.getStrings(ElasticConstants.HOSTS);
    port = parameters.getInt(ElasticConstants.PORT, DEFAULT_PORT);
    scheme = parameters.get(ElasticConstants.SCHEME, HttpHost.DEFAULT_SCHEME_NAME);
    auth = parameters.getBoolean(ElasticConstants.USE_AUTH, false);
    user = parameters.get(ElasticConstants.USER, DEFAULT_USER);
    password = parameters.get(ElasticConstants.PASSWORD, "");

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(user, password));

    if (hosts == null || port <= 1) {
      throw new IOException(
          "ElasticRestClient initialization Failed!!!\\n\\nPlease Provide the hosts");
    }

    HttpHost[] hostsList = new HttpHost[hosts.length];
    int i = 0;
    for (String host : hosts) {
      hostsList[i++] = new HttpHost(host, port, scheme);
    }
    RestClientBuilder restClientBuilder = RestClient.builder(hostsList);

    if (auth) {
      restClientBuilder
          .setHttpClientConfigCallback(new HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(
                HttpAsyncClientBuilder arg0) {
              return arg0.setDefaultCredentialsProvider(credentialsProvider);
            }
          });
    }

    // In case of HTTPS, set the client up for ignoring problems with self-signed
    // certificates and stuff.
    if ("https".equals(scheme)) {
      try {
        SSLContextBuilder sslBuilder = SSLContexts.custom();
        sslBuilder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
        final SSLContext sslContext = sslBuilder.build();

        restClientBuilder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
          @Override
          public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
            if (auth) {
              httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
            // ignore issues with self-signed certificates
            httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
            return httpClientBuilder.setSSLContext(sslContext);
          }
        });
      } catch (Exception e) {
        LOG.error("Error setting up SSLContext because: {}", e.getMessage(), e);
      }
    }

    return restClientBuilder.build();
  }

  /**
   * Generates a default BulkIngester listener.
   *
   * @return {@link BulkListener}
   */
  protected BulkListener<RetryContext> bulkListener() {
    return new BulkListener<RetryContext>() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request,
          List<RetryContext> contexts) {
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
          List<RetryContext> contexts, Throwable failure) {
        LOG.error("Elasticsearch indexing failed:", failure);
        for (RetryContext context : contexts) {
          scheduleRetry(context, "bulk request failure: " + failure.getMessage());
        }
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
          List<RetryContext> contexts, BulkResponse response) {
        if (!response.errors()) {
          return;
        }
        int loggedFailures = 0;
        List<BulkResponseItem> items = response.items();
        for (int i = 0; i < items.size() && i < contexts.size(); i++) {
          BulkResponseItem item = items.get(i);
          if (item.error() == null) {
            continue;
          }
          RetryContext context = contexts.get(i);
          String reason = item.error().reason();
          if (isRetryableStatus(item.status())) {
            scheduleRetry(context, "status " + item.status() + ": " + reason);
          } else {
            loggedFailures++;
            LOG.warn("Permanent Elasticsearch bulk failure for {}: status={}, type={}, reason={}",
                context.description, item.status(), item.error().type(), reason);
          }
        }
        if (loggedFailures > 0) {
          LOG.warn("Permanent failures occurred during bulk request: {}",
              loggedFailures);
        }
      }
    };
  }

  @Override
  public void write(NutchDocument doc) throws IOException {
    String id = (String) doc.getFieldValue("id");
    BulkOperation operation = buildIndexOperation(id, doc);
    addOperation(operation, "index document " + id);
  }

  @Override
  public void delete(String key) throws IOException {
    BulkOperation operation = buildDeleteOperation(key);
    addOperation(operation, "delete document " + key);
  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    write(doc);
  }

  @Override
  public void commit() throws IOException {
    bulkIngester.flush();
  }

  @Override
  public void close() throws IOException {
    IOException closeException = null;
    try {
      if (bulkIngester != null) {
        bulkIngester.flush();
        awaitBulkCompletion(bulkCloseTimeout, TimeUnit.SECONDS);
        bulkIngester.close();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("interrupted while waiting for BulkIngester to complete ({})",
          e.getMessage());
    } catch (RuntimeException e) {
      closeException = new IOException("Error closing BulkIngester", e);
    } finally {
      closing = true;
      shutdownRetryScheduler();
    }

    try {
      if (transport != null) {
        transport.close();
      }
    } catch (IOException e) {
      if (closeException == null) {
        closeException = e;
      } else {
        closeException.addSuppressed(e);
      }
    }

    if (closeException != null) {
      throw closeException;
    }
  }

  BulkOperation buildIndexOperation(String id, NutchDocument doc) {
    Map<String, Object> source = createSourceMap(doc);
    return BulkOperation.of(op -> op.index(index -> index
        .index(defaultIndex)
        .id(id)
        .document(source)));
  }

  BulkOperation buildDeleteOperation(String key) {
    return BulkOperation.of(op -> op.delete(delete -> delete
        .index(defaultIndex)
        .id(key)));
  }

  Map<String, Object> createSourceMap(NutchDocument doc) {
    Map<String, Object> source = new LinkedHashMap<>();
    for (final Map.Entry<String, NutchField> e : doc) {
      final List<Object> values = e.getValue().getValues();

      if (values.size() > 1) {
        List<Object> normalizedValues = new ArrayList<>(values.size());
        for (Object value : values) {
          normalizedValues.add(normalizeValue(value));
        }
        source.put(e.getKey(), normalizedValues);
      } else {
        source.put(e.getKey(), normalizeValue(values.get(0)));
      }
    }
    return source;
  }

  private static Object normalizeValue(Object value) {
    if (value instanceof java.util.Date) {
      return DateTimeFormatter.ISO_INSTANT
          .format(((java.util.Date) value).toInstant());
    }
    return value;
  }

  private void addOperation(BulkOperation operation, String description) {
    bulkIngester.add(operation, new RetryContext(operation, description, 0));
  }

  private void scheduleRetry(RetryContext context, String reason) {
    if (closing) {
      LOG.warn("Skipping Elasticsearch retry for {} because writer is closing: {}",
          context.description, reason);
      return;
    }
    if (context.attempt >= expBackoffRetries) {
      LOG.warn("Exhausted Elasticsearch bulk retries for {} after {} attempts: {}",
          context.description, context.attempt, reason);
      return;
    }

    long delayMillis = computeExponentialBackoffMillis(expBackoffMillis,
        context.attempt);
    RetryContext retryContext = context.nextAttempt();
    scheduledRetries.incrementAndGet();
    retryScheduler.schedule(() -> {
      try {
        bulkIngester.add(retryContext.operation, retryContext);
      } catch (RuntimeException e) {
        LOG.error("Elasticsearch retry failed for {}:", retryContext.description, e);
      } finally {
        scheduledRetries.decrementAndGet();
      }
    }, delayMillis, TimeUnit.MILLISECONDS);
    LOG.debug("Scheduled Elasticsearch retry for {} in {} ms after {}",
        context.description, delayMillis, reason);
  }

  private void awaitBulkCompletion(long timeout, TimeUnit unit)
      throws InterruptedException {
    long timeoutNanos = unit.toNanos(timeout);
    long deadline = System.nanoTime() + timeoutNanos;
    while (hasPendingBulkWork() && System.nanoTime() < deadline) {
      TimeUnit.MILLISECONDS.sleep(DRAIN_POLL_MILLIS);
    }

    if (hasPendingBulkWork()) {
      LOG.warn(
          "Timed out waiting for BulkIngester to complete. pendingOperations={}, pendingRequests={}, scheduledRetries={}",
          bulkIngester.pendingOperations(), bulkIngester.pendingRequests(),
          scheduledRetries.get());
    }
  }

  private boolean hasPendingBulkWork() {
    return bulkIngester != null
        && (bulkIngester.pendingOperations() > 0
        || bulkIngester.pendingRequests() > 0
        || scheduledRetries.get() > 0);
  }

  private void shutdownRetryScheduler() {
    if (retryScheduler == null) {
      return;
    }
    retryScheduler.shutdown();
    try {
      if (!retryScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        retryScheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      retryScheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  static boolean isRetryableStatus(int status) {
    return status == 429 || status == 502 || status == 503 || status == 504;
  }

  static long computeExponentialBackoffMillis(int initialDelayMillis,
      int completedAttempts) {
    if (initialDelayMillis <= 0) {
      return 0;
    }
    long delay = initialDelayMillis;
    for (int i = 0; i < completedAttempts; i++) {
      if (delay > Long.MAX_VALUE / 2) {
        return Long.MAX_VALUE;
      }
      delay *= 2;
    }
    return delay;
  }

  /**
   * Returns {@link Map} with the specific parameters the IndexWriter instance
   * can take.
   *
   * @return The values of each row. It must have the form
   *         &#60;KEY,&#60;DESCRIPTION,VALUE&#62;&#62;.
   */
  @Override
  public Map<String, Map.Entry<String, Object>> describe() {
    Map<String, Map.Entry<String, Object>> properties = new LinkedHashMap<>();

    properties.put(ElasticConstants.HOSTS,
        new AbstractMap.SimpleEntry<>("Comma-separated list of hostnames",
            this.hosts == null ? "" : String.join(",", hosts)));
    properties.put(ElasticConstants.PORT, new AbstractMap.SimpleEntry<>(
        "The port to connect to elastic server.", this.port));
    properties.put(ElasticConstants.SCHEME, new AbstractMap.SimpleEntry<>(
        "The scheme (http or https) to connect to elastic server.", this.scheme));
    properties.put(ElasticConstants.INDEX, new AbstractMap.SimpleEntry<>(
        "Default index to send documents to.", this.defaultIndex));
    properties.put(ElasticConstants.USER, new AbstractMap.SimpleEntry<>(
        "Username for auth credentials", this.user));
    properties.put(ElasticConstants.PASSWORD, new AbstractMap.SimpleEntry<>(
        "Password for auth credentials", StringUtil.mask(this.password)));
    properties.put(ElasticConstants.MAX_BULK_DOCS,
        new AbstractMap.SimpleEntry<>(
            "Maximum size of the bulk in number of documents.",
            this.maxBulkDocs));
    properties.put(ElasticConstants.MAX_BULK_LENGTH,
        new AbstractMap.SimpleEntry<>("Maximum size of the bulk in bytes.",
            this.maxBulkLength));
    properties.put(ElasticConstants.EXPONENTIAL_BACKOFF_MILLIS,
        new AbstractMap.SimpleEntry<>(
            "Initial delay for the BulkIngester retry policy.",
            this.expBackoffMillis));
    properties.put(ElasticConstants.EXPONENTIAL_BACKOFF_RETRIES,
        new AbstractMap.SimpleEntry<>(
            "Number of times the BulkIngester retry policy should retry retryable bulk operations.",
            this.expBackoffRetries));
    properties.put(ElasticConstants.BULK_CLOSE_TIMEOUT,
        new AbstractMap.SimpleEntry<>(
            "Number of seconds allowed for the BulkIngester to complete its last operation.",
            this.bulkCloseTimeout));

    return properties;
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  static final class RetryContext {
    private final BulkOperation operation;
    private final String description;
    private final int attempt;

    RetryContext(BulkOperation operation, String description, int attempt) {
      this.operation = operation;
      this.description = description;
      this.attempt = attempt;
    }

    RetryContext nextAttempt() {
      return new RetryContext(operation, description, attempt + 1);
    }
  }

  private static final class ElasticBulkThreadFactory implements ThreadFactory {
    private final AtomicInteger threadNumber = new AtomicInteger();

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = new Thread(runnable,
          "nutch-elastic-bulk-" + threadNumber.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    }
  }
}
