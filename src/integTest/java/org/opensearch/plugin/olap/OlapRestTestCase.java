/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hc.core5.http.HttpHost;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Base class for OLAP plugin integration tests. Runs against a real OpenSearch cluster with the SQL
 * and OLAP plugins installed. The cluster is managed by Gradle's {@code testClusters}.
 *
 * <p>Creates a plain REST client from the {@code tests.rest.cluster} system property instead of
 * using {@code OpenSearchRestTestCase} to avoid TLS/slf4j classpath issues.
 */
public abstract class OlapRestTestCase extends OpenSearchTestCase {

  protected static final String PPL_ENDPOINT = "/_plugins/_ppl";
  protected static final String TEST_INDEX = "test_olap";

  private static volatile RestClient restClient;

  @org.junit.AfterClass
  public static void cleanUpClient() throws IOException {
    if (restClient != null) {
      restClient.close();
      restClient = null;
    }
  }

  protected static RestClient client() {
    if (restClient == null) {
      String cluster = System.getProperty("tests.rest.cluster");
      if (cluster == null || cluster.isEmpty()) {
        throw new IllegalStateException(
            "tests.rest.cluster system property not set. Run via ./gradlew integTest");
      }
      HttpHost[] hosts =
          Arrays.stream(cluster.split(","))
              .map(
                  url -> {
                    int port = Integer.parseInt(url.substring(url.lastIndexOf(':') + 1));
                    String host = url.substring(0, url.lastIndexOf(':'));
                    return new HttpHost("http", host, port);
                  })
              .toArray(HttpHost[]::new);
      restClient = RestClient.builder(hosts).build();
    }
    return restClient;
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  /** Create the test index with sample data. */
  protected void createTestIndex() throws IOException {
    Request createIndex = new Request("PUT", "/" + TEST_INDEX);
    createIndex.setJsonEntity(
        "{"
            + "\"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 0},"
            + "\"mappings\": {\"properties\": {"
            + "\"name\": {\"type\": \"keyword\"},"
            + "\"age\": {\"type\": \"integer\"},"
            + "\"city\": {\"type\": \"keyword\"},"
            + "\"salary\": {\"type\": \"double\"}"
            + "}}"
            + "}");
    client().performRequest(createIndex);

    Request bulk = new Request("POST", "/_bulk?refresh=true");
    bulk.setJsonEntity(
        "{\"index\": {\"_index\": \"test_olap\"}}\n"
            + "{\"name\": \"Alice\", \"age\": 35, \"city\": \"Seattle\", \"salary\": 120000}\n"
            + "{\"index\": {\"_index\": \"test_olap\"}}\n"
            + "{\"name\": \"Bob\", \"age\": 28, \"city\": \"Portland\", \"salary\": 95000}\n"
            + "{\"index\": {\"_index\": \"test_olap\"}}\n"
            + "{\"name\": \"Charlie\", \"age\": 42, \"city\": \"Seattle\", \"salary\": 150000}\n"
            + "{\"index\": {\"_index\": \"test_olap\"}}\n"
            + "{\"name\": \"Diana\", \"age\": 31, \"city\": \"Denver\", \"salary\": 110000}\n"
            + "{\"index\": {\"_index\": \"test_olap\"}}\n"
            + "{\"name\": \"Eve\", \"age\": 26, \"city\": \"Portland\", \"salary\": 88000}\n");
    client().performRequest(bulk);
  }

  /** Delete the test index if it exists. */
  protected void deleteTestIndex() throws IOException {
    try {
      client().performRequest(new Request("DELETE", "/" + TEST_INDEX));
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() != 404) {
        throw e;
      }
    }
  }

  /** Execute a PPL query and return the JSON response. */
  protected JSONObject executePPLQuery(String query) throws IOException {
    Request request = new Request("POST", PPL_ENDPOINT);
    request.setJsonEntity("{\"query\": \"" + query + "\"}");
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    String body = new String(response.getEntity().getContent().readAllBytes());
    return new JSONObject(body);
  }

  /** Get datarows from a PPL response as a JSONArray. */
  protected JSONArray getDataRows(JSONObject response) {
    return response.getJSONArray("datarows");
  }
}
