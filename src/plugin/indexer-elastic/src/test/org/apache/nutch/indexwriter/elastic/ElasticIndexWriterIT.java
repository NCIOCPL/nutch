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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpHost;
import org.apache.nutch.indexer.AbstractIndexWriterIT;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.util.NutchConfiguration;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for ElasticIndexWriter using Testcontainers.
 */
@Testcontainers(disabledWithoutDocker = true)
public class ElasticIndexWriterIT extends AbstractIndexWriterIT {

  private static final String ELASTICSEARCH_IMAGE =
      "docker.elastic.co/elasticsearch/elasticsearch:8.16.6";
  private static final String TEST_INDEX = "test-index";

  @Container
  private static final ElasticsearchContainer elasticsearchContainer =
      new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
          .withEnv("discovery.type", "single-node")
          .withEnv("xpack.security.enabled", "false")
          .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");

  private ElasticIndexWriter indexWriter;
  private Configuration conf;

  @Override
  public void setUpIndexWriter() throws Exception {
    conf = NutchConfiguration.create();
    indexWriter = new ElasticIndexWriter();
    indexWriter.setConf(conf);

    Map<String, String> params = new HashMap<>();
    params.put(ElasticConstants.HOSTS, elasticsearchContainer.getHost());
    params.put(ElasticConstants.PORT, String.valueOf(elasticsearchContainer.getMappedPort(9200)));
    params.put(ElasticConstants.INDEX, TEST_INDEX);
    params.put(ElasticConstants.SCHEME, "http");

    IndexWriterParams writerParams = new IndexWriterParams(params);
    indexWriter.open(writerParams);
  }

  @Override
  public void tearDownIndexWriter() throws Exception {
    if (indexWriter != null) {
      try {
        indexWriter.close();
      } catch (Exception e) {
        // Ignore if open() failed and close state is invalid
      }
      indexWriter = null;
    }
  }

  @Override
  public IndexWriter getIndexWriter() {
    return indexWriter;
  }

  @Override
  public boolean supportsDelete() {
    return true;
  }

  @Override
  public void verifyDocumentWritten(String docId, String expectedTitle) throws Exception {
    GetResponse<Map> getResponse = getDocument(docId);
    assertTrue(getResponse.found(), "Document should exist in index");
    assertNotNull(getResponse.source());
    assertEquals(expectedTitle, getResponse.source().get("title"));
  }

  @Test
  void testWriteMultiValueField() throws Exception {
    NutchDocument doc = createTestDocument("test-doc-multi-value",
        "Multi Value Document", "");
    doc.add("tag", "one");
    doc.add("tag", "two");

    indexWriter.write(doc);
    indexWriter.commit();
    tearDownIndexWriter();

    GetResponse<Map> getResponse = getDocument("test-doc-multi-value");
    assertTrue(getResponse.found(), "Document should exist in index");
    Object tags = getResponse.source().get("tag");
    assertInstanceOf(List.class, tags);
    assertEquals(List.of("one", "two"), tags);
  }

  @Test
  void testDeleteRemovesDocument() throws Exception {
    String docId = "test-doc-elastic-delete";
    NutchDocument doc = createTestDocument(docId, "Document to Delete", "");

    indexWriter.write(doc);
    indexWriter.commit();
    indexWriter.delete(docId);
    indexWriter.commit();
    tearDownIndexWriter();

    GetResponse<Map> getResponse = getDocument(docId);
    assertFalse(getResponse.found(), "Document should be deleted from index");
  }

  private GetResponse<Map> getDocument(String docId) throws Exception {
    try (ElasticsearchTransport transport = new RestClientTransport(
        RestClient.builder(
            new HttpHost(elasticsearchContainer.getHost(),
                elasticsearchContainer.getMappedPort(9200),
                "http")).build(),
        new JacksonJsonpMapper())) {
      ElasticsearchClient client = new ElasticsearchClient(transport);
      return client.get(get -> get.index(TEST_INDEX).id(docId), Map.class);
    }
  }
}
