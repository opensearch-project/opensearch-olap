/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.olap;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Integration tests for PPL UDF → Velox function mapping. Verifies that PPL queries using built-in
 * functions (math, string, trig, conditional) are executed through Velox and produce correct
 * results.
 */
public class FunctionIT extends OlapRestTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    loadIndex(Index.EMPLOYEES);
  }

  @Override
  public void tearDown() throws Exception {
    deleteIndex(Index.EMPLOYEES.getName());
    super.tearDown();
  }

  // ---- String functions ----

  public void testUpper() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | eval upper_name = upper(name) | fields upper_name"
                + " | sort upper_name");
    JSONArray rows = getDataRows(response);
    assertEquals(5, rows.length());
    Set<String> names = extractColumn(rows, 0);
    assertTrue("ALICE should be in results", names.contains("ALICE"));
    assertTrue("BOB should be in results", names.contains("BOB"));
  }

  public void testLower() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | eval lower_name = lower(name) | fields lower_name"
                + " | sort lower_name");
    JSONArray rows = getDataRows(response);
    assertEquals(5, rows.length());
    Set<String> names = extractColumn(rows, 0);
    assertTrue("alice should be in results", names.contains("alice"));
    assertTrue("bob should be in results", names.contains("bob"));
  }

  public void testLength() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | eval name_len = length(name) | fields name, name_len"
                + " | sort name");
    JSONArray rows = getDataRows(response);
    Map<String, Integer> nameLengths = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      nameLengths.put(row.getString(0), row.getInt(1));
    }
    assertEquals(Integer.valueOf(5), nameLengths.get("Alice"));
    assertEquals(Integer.valueOf(3), nameLengths.get("Bob"));
    assertEquals(Integer.valueOf(7), nameLengths.get("Charlie"));
  }

  public void testSubstring() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | eval first_char = substring(name, 1, 1) | fields name,"
                + " first_char | sort name");
    JSONArray rows = getDataRows(response);
    Map<String, String> firstChars = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      firstChars.put(row.getString(0), row.getString(1));
    }
    assertEquals("A", firstChars.get("Alice"));
    assertEquals("B", firstChars.get("Bob"));
    assertEquals("C", firstChars.get("Charlie"));
  }

  public void testConcat() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | eval greeting = concat('Hello, ', name) | fields greeting"
                + " | sort greeting");
    JSONArray rows = getDataRows(response);
    Set<String> greetings = extractColumn(rows, 0);
    assertTrue(greetings.contains("Hello, Alice"));
    assertTrue(greetings.contains("Hello, Bob"));
  }

  public void testReverse() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | where name = 'Bob' | eval rev = reverse(name) | fields rev");
    JSONArray rows = getDataRows(response);
    assertEquals(1, rows.length());
    assertEquals("boB", rows.getJSONArray(0).getString(0));
  }

  // ---- Math functions ----

  public void testAbs() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | eval diff = abs(salary - 110000) | fields name, diff"
                + " | sort name");
    JSONArray rows = getDataRows(response);
    Map<String, Number> diffs = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      diffs.put(row.getString(0), row.getDouble(1));
    }
    assertEquals(10000.0, diffs.get("Alice").doubleValue(), 0.1);
    assertEquals(0.0, diffs.get("Diana").doubleValue(), 0.1);
  }

  public void testCeilAndFloor() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | eval c = ceil(salary / 100000), f = floor(salary / 100000)"
                + " | fields name, c, f | sort name");
    JSONArray rows = getDataRows(response);
    // Alice: salary=120000. ceil(1.2)=2, floor(1.2)=1
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      if ("Alice".equals(row.getString(0))) {
        assertEquals(2, row.getInt(1)); // ceil
        assertEquals(1, row.getInt(2)); // floor
      }
    }
  }

  public void testRound() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | eval rounded = round(salary / 1000) | fields name, rounded"
                + " | sort name");
    JSONArray rows = getDataRows(response);
    Map<String, Number> rounded = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      rounded.put(row.getString(0), row.getNumber(1));
    }
    assertEquals(120.0, rounded.get("Alice").doubleValue(), 0.1);
    assertEquals(95.0, rounded.get("Bob").doubleValue(), 0.1);
  }

  public void testPower() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | where name = 'Alice' | eval sq = power(dept_id, 2)"
                + " | fields sq");
    JSONArray rows = getDataRows(response);
    assertEquals(1, rows.length());
    assertEquals(100.0, rows.getJSONArray(0).getDouble(0), 0.1); // 10^2 = 100
  }

  public void testSqrt() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | where name = 'Alice' | eval root = sqrt(salary)"
                + " | fields root");
    JSONArray rows = getDataRows(response);
    assertEquals(1, rows.length());
    assertEquals(Math.sqrt(120000), rows.getJSONArray(0).getDouble(0), 0.1);
  }

  public void testSign() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | eval s = sign(salary - 100000) | fields name, s | sort name");
    JSONArray rows = getDataRows(response);
    Map<String, Integer> signs = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      signs.put(row.getString(0), row.getInt(1));
    }
    assertEquals(Integer.valueOf(1), signs.get("Alice")); // 120000 - 100000 > 0
    assertEquals(Integer.valueOf(-1), signs.get("Bob")); // 95000 - 100000 < 0
  }

  public void testExpAndLn() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | where name = 'Alice' | eval e = exp(1.0), l = ln(exp(1.0))"
                + " | fields e, l");
    JSONArray rows = getDataRows(response);
    assertEquals(1, rows.length());
    assertEquals(Math.E, rows.getJSONArray(0).getDouble(0), 0.01);
    assertEquals(1.0, rows.getJSONArray(0).getDouble(1), 0.01);
  }

  // ---- Trigonometric functions ----

  public void testCosAndSin() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | where name = 'Alice' | eval c = cos(0.0), s = sin(0.0)"
                + " | fields c, s");
    JSONArray rows = getDataRows(response);
    assertEquals(1, rows.length());
    assertEquals(1.0, rows.getJSONArray(0).getDouble(0), 0.01);
    assertEquals(0.0, rows.getJSONArray(0).getDouble(1), 0.01);
  }

  public void testRadiansAndDegrees() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | where name = 'Alice'"
                + " | eval r = radians(180.0), d = degrees(3.141592653589793)"
                + " | fields r, d");
    JSONArray rows = getDataRows(response);
    assertEquals(1, rows.length());
    assertEquals(Math.PI, rows.getJSONArray(0).getDouble(0), 0.01);
    assertEquals(180.0, rows.getJSONArray(0).getDouble(1), 0.01);
  }

  // ---- Conditional functions ----

  public void testCoalesce() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | eval safe_name = coalesce(name, 'Unknown')"
                + " | fields safe_name | sort safe_name");
    JSONArray rows = getDataRows(response);
    assertEquals(5, rows.length());
    Set<String> names = extractColumn(rows, 0);
    assertTrue(names.contains("Alice"));
    assertFalse(names.contains("Unknown")); // no nulls in test data
  }

  public void testIfFunction() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | eval label = if(salary > 100000, 'high', 'low')"
                + " | fields name, label | sort name");
    JSONArray rows = getDataRows(response);
    Map<String, String> labels = new HashMap<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      labels.put(row.getString(0), row.getString(1));
    }
    assertEquals("high", labels.get("Alice")); // 120000 > 100000
    assertEquals("low", labels.get("Bob")); // 95000 <= 100000
    assertEquals("high", labels.get("Charlie")); // 150000 > 100000
    assertEquals("high", labels.get("Diana")); // 110000 > 100000
    assertEquals("low", labels.get("Eve")); // 88000 <= 100000
  }

  // ---- Filter with functions ----

  public void testFilterWithUpper() throws IOException {
    JSONObject response =
        executePPLQuery("source = employees | where upper(name) = 'ALICE' | fields name, salary");
    JSONArray rows = getDataRows(response);
    assertEquals(1, rows.length());
    assertEquals("Alice", rows.getJSONArray(0).getString(0));
  }

  public void testFilterWithMathFunction() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | where abs(salary - 110000) < 20000 | fields name | sort name");
    JSONArray rows = getDataRows(response);
    // salary within 20000 of 110000: Alice(120000), Bob(95000), Diana(110000)
    Set<String> names = extractColumn(rows, 0);
    assertTrue(names.contains("Alice"));
    assertTrue(names.contains("Bob"));
    assertTrue(names.contains("Diana"));
    assertFalse(names.contains("Eve")); // 88000, diff=22000 > 20000
  }

  // ---- Combined: function in eval + aggregation ----

  public void testFunctionWithAggregation() throws IOException {
    JSONObject response =
        executePPLQuery(
            "source = employees | eval high_sal = if(salary > 100000, 1, 0)"
                + " | stats sum(high_sal) as high_count");
    JSONArray rows = getDataRows(response);
    assertEquals(1, rows.length());
    assertEquals(3, rows.getJSONArray(0).getInt(0)); // Alice, Charlie, Diana
  }

  // ---- Helpers ----

  private Set<String> extractColumn(JSONArray rows, int colIndex) {
    Set<String> values = new HashSet<>();
    for (int i = 0; i < rows.length(); i++) {
      values.add(rows.getJSONArray(i).getString(colIndex));
    }
    return values;
  }
}
