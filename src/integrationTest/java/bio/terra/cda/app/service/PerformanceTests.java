package bio.terra.cda.app.service;

import bio.terra.cda.app.model.QueryResult;
import bio.terra.cda.app.model.SchemaObjectList;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

public class PerformanceTests {

    String sql =
            "SELECT * FROM `gdc-bq-sample.cda_mvp.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` WHERE table_name = 'v3' Limit 1";
    /**
     * Test the SQL-QUERY API with formatted SQL. curl -X POST
     * https://cda.cda-dev.broadinstitute.org/api/v1/sql-query
     */
    @Test
    public void testSqlQueryApiIT(String sql) throws Exception {

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
