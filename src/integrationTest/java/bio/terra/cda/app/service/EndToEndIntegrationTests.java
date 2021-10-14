package bio.terra.cda.app.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import bio.terra.cda.app.model.QueryResult;
import bio.terra.cda.app.model.SchemaObjectList;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class EndToEndIntegrationTests {

  @Test
  public void testApiStatusIT() throws Exception {

    /* curl -X GET "https://cda.cda-dev.broadinstitute.org/status" -H "accept: application/json" */
    try {
      // create the process
      ProcessBuilder build =
          new ProcessBuilder(
              "/usr/bin/curl",
              "-X",
              "GET",
              "-H",
              "accept: application/json",
              "https://cda.cda-dev.broadinstitute.org/status");
      Process process = build.start();
      InputStream inputStream = process.getInputStream();

      ObjectMapper mapper = new ObjectMapper();
      Map<String, String> result = mapper.readValue(inputStream, Map.class);
      assertThat(result.toString(), containsString("everything is fine"));
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  /** curl -X POST "https://cda.cda-dev.broadinstitute.org/api/v1/unique-values/v3? */
  // @Disabled
  @Test
  public void testUniqueValuesApiIT() throws Exception {
    try {
      // create the process
      ProcessBuilder build =
          new ProcessBuilder(
              "/usr/bin/curl",
              "-X",
              "POST",
              "-H",
              "Content-Type: text/plain",
              "-H",
              "accept: application/json",
              "https://cda.cda-dev.broadinstitute.org/api/v1/unique-values/v3?tablename=gdc-bq-sample.cda_mvp",
              "-d",
              "sex");
      Process process = build.start();
      InputStream inputStream = process.getInputStream();

      ObjectMapper mapper = new ObjectMapper();
      QueryResult qr = mapper.readValue(inputStream, QueryResult.class);
      System.out.println(qr.getQuery_id());

      String result = retrieveQueryIdResults(qr.getQuery_id());
      assertThat(result, containsString("female"));

    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  /**
   * Test the SQL-QUERY API with formatted SQL. curl -X POST
   * https://cda.cda-dev.broadinstitute.org/api/v1/sql-query
   */
  @Test
  public void testSqlQueryApiIT() throws Exception {
    String sql =
        "SELECT * FROM `gdc-bq-sample.cda_mvp.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` WHERE table_name = 'v3' Limit 1";
    try {
      // create the process
      ProcessBuilder build =
          new ProcessBuilder(
              "/usr/bin/curl",
              "-X",
              "POST",
              "-H",
              "Content-Type: text/plain",
              "-H",
              "accept: application/json",
              "https://cda.cda-dev.broadinstitute.org/api/v1/sql-query",
              "-d",
              sql);
      Process process = build.start();
      InputStream inputStream = process.getInputStream();

      ObjectMapper mapper = new ObjectMapper();
      QueryResult qr = mapper.readValue(inputStream, QueryResult.class);

      SchemaObjectList results = retrieveQueryMapResults(qr.getQuery_id());

      assertThat(results.getResult().get(0).getColumn_name(), containsString("days_to_birth"));

    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  /**
   * Test the Boolean API with formatted json. curl -X POST
   * https://cda.cda-dev.broadinstitute.org/api/v1/boolean-query
   */
  @Test
  public void testBooleanQueryApiIT() throws Exception {
    String booleanQuery =
        "{\"node_type\":\"AND\",\"l\":{\"node_type\":\"AND\",\"l\":{\"node_type\":\">\",\"l\":{\"node_type\":\"column\",\"value\":\"ResearchSubject.Diagnosis.age_at_diagnosis\"},\"r\":{\"node_type\":\"unquoted\",\"value\":\"50 * 365\"}},\"r\":{\"node_type\":\"=\",\"l\":{\"node_type\":\"column\",\"value\":\"ResearchSubject.Specimen.associated_project\"},\"r\":{\"node_type\":\"quoted\",\"value\":\"TCGA-ESCA\"}}},\"r\":{\"node_type\":\"=\",\"l\":{\"node_type\":\"column\",\"value\":\"ResearchSubject.Diagnosis.tumor_stage\"},\"r\":{\"node_type\":\"quoted\",\"value\":\"stage iiic\"}}}";

    try {
      // create the process
      ProcessBuilder build =
          new ProcessBuilder(
              "/usr/bin/curl",
              "-X",
              "POST",
              "-H",
              "Content-Type: text/plain",
              "-H",
              "accept: application/json",
              "https://cda.cda-dev.broadinstitute.org/api/v1/boolean-query/v3?table=gdc-bq-sample.cda_mvp",
              "-d",
              booleanQuery);
      Process process = build.start();
      InputStream inputStream = process.getInputStream();
      System.out.println("command: " + build.command());

      ObjectMapper mapper = new ObjectMapper();
      QueryResult qr = mapper.readValue(inputStream, QueryResult.class);

      System.out.println(qr.getQuery_id());

      JsonNode jsonMap = retrieveQueryJsonResults(qr.getQuery_id());

      System.out.println(jsonMap.get(0).toString());
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  /**
   * This method retrieves the ResultSet from the Database using the query_id curl -X GET
   * "https://cda.cda-dev.broadinstitute.org/api/v1/query/<QUERY_ID>
   */
  private String retrieveQueryIdResults(String query_id) {
    Map<String, String> qr = null;
    try {
      // create the process
      ProcessBuilder build =
          new ProcessBuilder(
              "/usr/bin/curl",
              "-X",
              "GET",
              "-H",
              "accept: application/json",
              "https://cda.cda-dev.broadinstitute.org/api/v1/query/" + query_id + "?limit=100");

      Process process = build.start();
      InputStream inputStream = process.getInputStream();

      ObjectMapper mapper = new ObjectMapper();
      qr = mapper.readValue(inputStream, Map.class);
      System.out.println("command: " + build.command());
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return qr.toString();
  }

  /**
   * This method retrieves the ResultSet from the Database using the query_id curl -X GET
   * "https://cda.cda-dev.broadinstitute.org/api/v1/query/<QUERY_ID>
   */
  private SchemaObjectList retrieveQueryMapResults(String query_id) {
    SchemaObjectList results = null;
    try {
      // create the process
      ProcessBuilder build =
          new ProcessBuilder(
              "/usr/bin/curl",
              "-X",
              "GET",
              "-H",
              "accept: application/json",
              "https://cda.cda-dev.broadinstitute.org/api/v1/query/" + query_id + "?limit=100");

      Process process = build.start();
      InputStream inputStream = process.getInputStream();

      ObjectMapper mapper = new ObjectMapper();
      results = mapper.readValue(inputStream, SchemaObjectList.class);
      System.out.println("command: " + build.command());
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return results;
  }

  /**
   * This method retrieves the ResultSet from the Database using the query_id curl -X GET
   * "https://cda.cda-dev.broadinstitute.org/api/v1/query/<QUERY_ID>
   */
  private JsonNode retrieveQueryJsonResults(String query_id) {
    JsonNode jsonMap = null;
    JsonNode results = null;
    try {
      // create the process
      ProcessBuilder build =
          new ProcessBuilder(
              "/usr/bin/curl",
              "-X",
              "GET",
              "-H",
              "accept: application/json",
              "https://cda.cda-dev.broadinstitute.org/api/v1/query/" + query_id + "?limit=100");

      Process process = build.start();
      InputStream inputStream = process.getInputStream();

      ObjectMapper mapper = new ObjectMapper();
      jsonMap = mapper.readTree(inputStream);
      results = jsonMap.get("result");
      System.out.println("command: " + build.command());

    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return results;
  }
}
