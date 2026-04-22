/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
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
 * <p>Provides shared test index definitions via the {@link Index} enum. Each test loads indices by
 * calling {@link #loadIndex(Index)}, which creates the index from a mapping file and bulk-loads
 * data from a data file (both under {@code src/integTest/resources/}).
 */
public abstract class OlapRestTestCase extends OpenSearchTestCase {

  protected static final String PPL_ENDPOINT = "/_plugins/_ppl";

  private static volatile RestClient restClient;

  /** Shared test index definitions. Mapping and data files live under integTest/resources/. */
  public enum Index {
    TEST_OLAP("test_olap", "test_olap_mapping.json", "test_olap_data.json"),
    EMPLOYEES("employees", "employees_mapping.json", "employees_data.json"),
    DEPARTMENTS("departments", "departments_mapping.json", "departments_data.json"),
    PROJECTS("projects", "projects_mapping.json", "projects_data.json"),
    BIG5("big5", "big5_mapping.json", "big5_data.json"),
    NESTED_SIMPLE(
        "opensearch-sql_test_index_nested_simple",
        "nested_simple_mapping.json",
        "nested_simple_data.json"),
    DEEP_NESTED(
        "opensearch-sql_test_index_deep_nested",
        "deep_nested_mapping.json",
        "deep_nested_data.json");

    private final String indexName;
    private final String mappingFile;
    private final String dataFile;

    Index(String indexName, String mappingFile, String dataFile) {
      this.indexName = indexName;
      this.mappingFile = mappingFile;
      this.dataFile = dataFile;
    }

    public String getName() {
      return indexName;
    }

    public String getMappingFile() {
      return mappingFile;
    }

    public String getDataFile() {
      return dataFile;
    }
  }

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

  private static volatile boolean forceVectorizeSet = false;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    // Enable force_vectorize once per test run so all queries go through Velox.
    // If a query uses unsupported types/functions, it fails explicitly instead of
    // silently falling back to the default engine.
    if (!forceVectorizeSet) {
      setClusterSetting("plugins.velox.force_vectorize", true);
      forceVectorizeSet = true;
    }
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  // ---- Index loading ----

  /**
   * Load a test index: create it from the mapping file and bulk-insert data. Idempotent — if the
   * index already exists, this is a no-op.
   */
  protected void loadIndex(Index index) throws IOException {
    createIndex(index.getName(), readResource(index.getMappingFile()));
    bulkInsert(index.getName(), readResource(index.getDataFile()));
  }

  /**
   * Load a test index with a custom number of primary shards. The mapping file's settings are
   * overridden with the specified shard count.
   */
  protected void loadIndex(Index index, int numShards) throws IOException {
    String mapping = readResource(index.getMappingFile());
    JSONObject json = new JSONObject(mapping);
    JSONObject settings = new JSONObject();
    settings.put("number_of_shards", numShards);
    settings.put("number_of_replicas", 0);
    json.put("settings", settings);
    createIndex(index.getName(), json.toString());
    bulkInsert(index.getName(), readResource(index.getDataFile()));
  }

  /** Safely delete an index (ignores 404 if index doesn't exist). */
  protected void deleteIndex(String indexName) throws IOException {
    try {
      client().performRequest(new Request("DELETE", "/" + indexName));
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() != 404) {
        throw e;
      }
    }
  }

  private boolean indexExists(String indexName) {
    try {
      Response response = client().performRequest(new Request("HEAD", "/" + indexName));
      return response.getStatusLine().getStatusCode() == 200;
    } catch (ResponseException e) {
      return false;
    } catch (IOException e) {
      return false;
    }
  }

  private void createIndex(String indexName, String mapping) throws IOException {
    Request request = new Request("PUT", "/" + indexName);
    // Ensure settings include shards=1, replicas=0 if not already specified
    JSONObject json = new JSONObject(mapping);
    if (!json.has("settings")) {
      JSONObject settings = new JSONObject();
      settings.put("number_of_shards", 1);
      settings.put("number_of_replicas", 0);
      json.put("settings", settings);
    }
    request.setJsonEntity(json.toString());
    client().performRequest(request);
  }

  private void bulkInsert(String indexName, String bulkData) throws IOException {
    Request request = new Request("POST", "/" + indexName + "/_bulk?refresh=true");
    request.setJsonEntity(bulkData);
    client().performRequest(request);
  }

  private static final String RESOURCE_DIR;

  static {
    String root = System.getProperty("tests.project.root", ".");
    RESOURCE_DIR = root + "/src/integTest/resources/";
  }

  /** Read a resource file directly from the filesystem. */
  protected String readResource(String fileName) throws IOException {
    return Files.readString(Path.of(RESOURCE_DIR + fileName));
  }

  // ---- Query execution ----

  /** Execute a PPL query and return the JSON response. */
  protected JSONObject executePPLQuery(String query) throws IOException {
    return executePPLQuery(query, false);
  }

  /** Execute a PPL query, optionally with the {@code profile=true} body flag. */
  protected JSONObject executePPLQuery(String query, boolean profile) throws IOException {
    Request request = new Request("POST", PPL_ENDPOINT);
    // Use JSONObject to properly escape the query string (handles newlines, quotes, etc.)
    JSONObject payload = new JSONObject();
    payload.put("query", query);
    if (profile) {
      payload.put("profile", true);
    }
    request.setJsonEntity(payload.toString());
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    String body = new String(response.getEntity().getContent().readAllBytes());
    return new JSONObject(body);
  }

  /** Get datarows from a PPL response as a JSONArray. */
  protected JSONArray getDataRows(JSONObject response) {
    return response.getJSONArray("datarows");
  }

  /**
   * Read lines from the cluster log file that match the given substring. Used to verify that
   * predicate pushdown actually reached Lucene (vs being evaluated by Velox after a full scan).
   */
  protected List<String> getLogLines(String substring) throws IOException {
    String logPath = System.getProperty("tests.cluster.logfile");
    if (logPath == null || logPath.isEmpty()) {
      return List.of();
    }
    Path path = Path.of(logPath);
    if (!Files.exists(path)) {
      return List.of();
    }
    return Files.readAllLines(path).stream()
        .filter(line -> line.contains(substring))
        .collect(Collectors.toList());
  }

  /** Count how many log lines contain the given substring. */
  protected long countLogLines(String substring) throws IOException {
    return getLogLines(substring).size();
  }

  /**
   * Execute explain on a PPL query and return the Velox physical plan string. Uses the OLAP
   * plugin's explain endpoint which calls PlanNode.toFormatString(true, true).
   *
   * @return the Velox plan tree (from calcite.physical field), or the full response if not
   *     available
   */
  protected String explainVeloxPlan(String pplQuery) throws IOException {
    JSONObject response = executePPLQuery("explain " + pplQuery);
    if (response.has("calcite")) {
      JSONObject calcite = response.getJSONObject("calcite");
      if (calcite.has("physical")) {
        return calcite.getString("physical");
      }
    }
    return response.toString();
  }

  // ---- Cluster settings ----

  /** Update a persistent cluster setting. */
  protected void setClusterSetting(String key, String value) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity("{\"persistent\": {\"" + key + "\": \"" + value + "\"}}");
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /** Update a persistent cluster setting (boolean). */
  protected void setClusterSetting(String key, boolean value) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity("{\"persistent\": {\"" + key + "\": " + value + "}}");
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
  }
}
