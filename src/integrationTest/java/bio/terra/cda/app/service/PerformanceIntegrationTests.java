package bio.terra.cda.app.service;

import bio.terra.cda.app.model.QueryResult;
import bio.terra.cda.app.model.SchemaObjectList;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PerformanceIntegrationTests {
  private List<String> queryList;
  private long totalExecutionTime = 0;
  SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  Date date = new Date();

  private final String sql1 =
      "SELECT * FROM `gdc-bq-sample.cda_mvp.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` "
          + "WHERE table_name = 'v3'";

  private final String sql2 =
      "SELECT all_v2_1.* FROM gdc-bq-sample.integration.all_v2_1 AS all_v2_1 "
          + "WHERE (all_v2_1.id = 'TCGA-E2-A10A')";
  private final String sql3 =
      "SELECT all_v2_1.* FROM (SELECT all_v2_1.* FROM (SELECT all_v2_1.* FROM gdc-bq-sample.integration.all_v2_1 AS all_v2_1, "
          + "UNNEST(ResearchSubject) AS _ResearchSubject, UNNEST(_ResearchSubject.identifier) AS _identifier "
          + "WHERE (_identifier.system = 'GDC')) AS all_v2_1, UNNEST(ResearchSubject) AS _ResearchSubject, "
          + "UNNEST(_ResearchSubject.identifier) AS _identifier "
          + "WHERE (_identifier.system = 'PDC')) AS all_v2_1, UNNEST(identifier) AS _identifier "
          + "WHERE (_identifier.system = 'IDC')";
  private final String sql4 =
      "SELECT COUNT(all_v2_1) FROM"
          + "  gdc-bq-sample.integration.all_v2_1 AS all_v2_1\n"
          + "WHERE  (all_v2_1.id = 'TCGA-E2-A10A')";
  private final String sql5 =
      "SELECT(SUM((SELECT COUNT(system) FROM UNNEST(identifier) WHERE system = 'GDC'))) AS GDC,"
          + "(SUM((SELECT COUNT(system) FROM UNNEST(identifier) WHERE system = 'PDC'))) AS PDC,"
          + "(SUM((SELECT COUNT(system) FROM UNNEST(identifier) WHERE system = 'IDC'))) "
          + "AS IDC From (SELECT identifier FROM gdc-bq-sample.integration.all_v2_1), UNNEST(identifier)";
  private final String sql6 =
      "SELECT all_v2_1.* FROM gdc-bq-sample.integration.all_v2_1 AS all_v2_1, "
          + "UNNEST(ResearchSubject) AS _ResearchSubject, "
          + "UNNEST(_ResearchSubject.Specimen) AS _Specimen, "
          + "UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis, "
          + "UNNEST(_Specimen.identifier) AS _identifier "
          + "WHERE ((_Specimen.primary_disease_type = 'Nevi and Melanomas') "
          + "AND ((_Diagnosis.age_at_diagnosis < 30*365) AND (_identifier.system = 'GDC')))";
  private final String sql7 =
      "SELECT DISTINCT _Specimen.associated_project FROM integration.all_v2_1, "
          + "UNNEST(ResearchSubject) AS _ResearchSubject, "
          + "UNNEST(_ResearchSubject.Specimen) "
          + "AS _Specimen ORDER BY _Specimen.associated_project";
  private final String sql8 =
      "SELECT all_v2_1.* FROM gdc-bq-sample.integration.all_v2_1 AS all_v2_1, "
          + "UNNEST(ResearchSubject) AS _ResearchSubject, "
          + "UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis "
          + "WHERE ((_Diagnosis.age_at_diagnosis > 50*365) "
          + "AND (_ResearchSubject.member_of_research_project = 'TCGA-OV'))";
  private final String sql9 =
      "SELECT DISTINCT vital_status FROM integration.all_v2_1 ORDER BY vital_status";
  private final String sql10 =
      "SELECT all_v2_1.* FROM gdc-bq-sample.integration.all_v2_1 AS all_v2_1, "
          + "UNNEST(ResearchSubject) AS _ResearchSubject "
          + "WHERE ((all_v2_1.sex = 'female') "
          + "AND ((_ResearchSubject.primary_diagnosis_condition = 'Breast Invasive Carcinoma') "
          + "AND ((all_v2_1.days_to_birth <= -30*365) AND (all_v2_1.days_to_birth >= -45*365))))";

  @BeforeAll
  public void init() {
    queryList = new ArrayList<String>();
    queryList.add(sql1);
    queryList.add(sql2);
    queryList.add(sql3);
    queryList.add(sql4);
    queryList.add(sql5);
    queryList.add(sql6);
    queryList.add(sql7);
    queryList.add(sql8);
    queryList.add(sql9);
    queryList.add(sql10);
  }

  @Test
  public void runPerformanceTests() throws Exception {
    for (String query : queryList) {
      long startTime = System.currentTimeMillis();
      runSql(query);
      long endTime = System.currentTimeMillis();
      long totalTime = endTime - startTime;
      System.out.print("Query: " + query);
      System.out.println("\nExecution time in milliseconds  : " + totalTime + "ms\n");
      totalExecutionTime += totalTime;
    }
    System.out.println(
        "\nPerformance Report for "
            + dateFormat.format(date)
            + " Optimal performance is <= 80secs");
    System.out.println(
        "*** Total Execution for 10 sequential queries in seconds: "
            + totalExecutionTime / 1000
            + "secs");
  }

  /**
   * Test the SQL-QUERY API with formatted SQL. curl -X POST
   * https://cda.cda-dev.broadinstitute.org/api/v1/sql-query
   */
  public void runSql(String sql) {

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
      System.out.println(qr.toString());
      retrieveQueryMapResults(qr.getQuery_id());

    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
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
      System.out.println("RESULTS Row Length: " + results.getResult().size());
      System.out.println("Results Total Row Count: " + results.getTotal_row_count());
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return results;
  }
}
