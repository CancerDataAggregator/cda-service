package bio.terra.cda.app.service;

import bio.terra.cda.app.model.QueryResult;
import bio.terra.cda.app.model.SchemaObjectList;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class PerformanceTests {

   String sql1 =
            "SELECT * FROM `gdc-bq-sample.cda_mvp.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` " +
                    "WHERE table_name = 'v3' Limit 1";
   String sql2 =
            "SELECT all_v2_1.* FROM gdc-bq-sample.integration.all_v2_1 AS all_v2_1 " +
                    "WHERE (all_v2_1.id = 'TCGA-E2-A10A')";
   String sql3 =
            "SELECT all_v2_1.* FROM (SELECT all_v2_1.* FROM (SELECT all_v2_1.* FROM gdc-bq-sample.integration.all_v2_1 AS all_v2_1, " +
                    "UNNEST(ResearchSubject) AS _ResearchSubject, UNNEST(_ResearchSubject.identifier) AS _identifier " +
                    "WHERE (_identifier.system = 'GDC')) AS all_v2_1, UNNEST(ResearchSubject) AS _ResearchSubject, " +
                    "UNNEST(_ResearchSubject.identifier) AS _identifier " +
                    "WHERE (_identifier.system = 'PDC')) AS all_v2_1, UNNEST(identifier) AS _identifier " +
                    "WHERE (_identifier.system = 'IDC')";
   String sql4 =
            "SELECT COUNT(all_v2_1) FROM" +
                    "  gdc-bq-sample.integration.all_v2_1 AS all_v2_1\n" +
                    "WHERE  (all_v2_1.id = 'TCGA-E2-A10A')";
   String sql5 =
            "SELECT(SUM((SELECT COUNT(system) FROM UNNEST(identifier) WHERE system = 'GDC'))) AS GDC," +
                    "(    SUM((SELECT COUNT(system) FROM UNNEST(identifier) WHERE system = 'PDC'))) AS PDC," +
                    "(    SUM((SELECT COUNT(system) FROM UNNEST(identifier) WHERE system = 'IDC'))) " +
                    "AS IDC From (SELECT identifier FROM gdc-bq-sample.integration.all_v2_1), UNNEST(identifier)";
   String sql6 =
            "SELECT all_v2_1.* FROM gdc-bq-sample.integration.all_v2_1 AS all_v2_1, " +
                    "UNNEST(ResearchSubject) AS _ResearchSubject, " +
                    "UNNEST(_ResearchSubject.Specimen) AS _Specimen, " +
                    "UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis, " +
                    "UNNEST(_Specimen.identifier) AS _identifier " +
                    "WHERE ((_Specimen.primary_disease_type = 'Nevi and Melanomas') " +
                    "AND ((_Diagnosis.age_at_diagnosis < 30*365) AND (_identifier.system = 'GDC')))";
   String sql7 =
            "SELECT DISTINCT _Specimen.associated_project FROM integration.all_v2_1, " +
                    "UNNEST(ResearchSubject) AS _ResearchSubject, " +
                    "UNNEST(_ResearchSubject.Specimen) " +
                    "AS _Specimen ORDER BY _Specimen.associated_project";
   String sql8 =
            "SELECT all_v2_1.* FROM gdc-bq-sample.integration.all_v2_1 AS all_v2_1, " +
                    "UNNEST(ResearchSubject) AS _ResearchSubject, " +
                    "UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis " +
                    "WHERE ((_Diagnosis.age_at_diagnosis > 50*365) " +
                    "AND (_ResearchSubject.associated_project = 'TCGA-OV'))";
   String sql9 =
            "SELECT DISTINCT vital_status FROM integration.all_v2_1 ORDER BY vital_status";
   String sql10 =
            "SELECT all_v2_1.* FROM gdc-bq-sample.integration.all_v2_1 AS all_v2_1, " +
                    "UNNEST(ResearchSubject) AS _ResearchSubject " +
                    "WHERE ((all_v2_1.sex = 'female') " +
                    "AND ((_ResearchSubject.primary_disease_type = 'Breast Invasive Carcinoma') " +
                    "AND ((all_v2_1.days_to_birth <= -30*365) AND (all_v2_1.days_to_birth >= -45*365))))";

   @Test
   public void runPerformanceTests() throws Exception {
       List<String> queryList = new ArrayList<String>();
       for(int i = 1; i == 10; i++) {
           queryList.add("sql"+i);
       }

       for (String query: queryList) {
           long startTime = System.currentTimeMillis();
           runSql(query);
           long endTime = System.currentTimeMillis();
           long totalTime = endTime - startTime;
           System.out.print("Query: "+ query);
           System.out.println("Execution time in milliseconds  : " + totalTime);
       }
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

            SchemaObjectList results = retrieveQueryMapResults(qr.getQuery_id());

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
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return results;
     }
}
