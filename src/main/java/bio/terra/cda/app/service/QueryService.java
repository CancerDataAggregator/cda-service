package bio.terra.cda.app.service;

import bio.terra.cda.app.service.exception.BadQueryException;
import bio.terra.cda.app.util.QueryTranslator;
import bio.terra.cda.generated.model.Query;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class QueryService {

  public static class QueryResult {

    public final Query query;
    public final String querySql;
    public final List<String> result;

    public QueryResult(Query query, String querySql, List<String> result) {
      this.query = query;
      this.querySql = querySql;
      this.result = result;
    }
  }

  final BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
  private final String table;

  public QueryService(String table) {
    this.table = table;
  }

  private Job runJob(Job queryJob) {
    try {
      // Wait for the query to complete.
      queryJob = queryJob.waitFor();
    } catch (InterruptedException e) {
      throw new RuntimeException("Error while polling for job completion", e);
    }

    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }
    return queryJob;
  }

  private List<String> getJobResults(Job queryJob) {
    try {
      // Get the results.
      TableResult result = queryJob.getQueryResults();

      List<String> jsonData = new ArrayList<>();

      // Print all pages of the results.
      for (FieldValueList row : result.iterateAll()) {
        jsonData.add(row.get(0).getStringValue());
      }

      return jsonData;
    } catch (InterruptedException e) {
      throw new RuntimeException("Error while getting query results", e);
    }
  }

  public QueryResult runQuery(Query query, Integer offset, Integer limit) {

    String queryString = new QueryTranslator(this.table, query).sql();
    offset = Objects.requireNonNullElse(offset, 0);
    limit = Objects.requireNonNullElse(limit, 100);
    String queryStringWithPagination =
        String.format("%s LIMIT %s OFFSET %s", queryString, limit, offset);

    // Wrap query so it returns JSON
    String jsonQuery =
        String.format("SELECT TO_JSON_STRING(t,true) from (%s) as t", queryStringWithPagination);
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(jsonQuery).setUseLegacySql(false).build();

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());

    try {
      Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
      queryJob = runJob(queryJob);
      return new QueryResult(query, queryStringWithPagination, getJobResults(queryJob));
    } catch (Throwable t) {
      throw new BadQueryException(
          String.format("Error calling BigQuery: '%s'", queryStringWithPagination), t);
    }
  }
}
