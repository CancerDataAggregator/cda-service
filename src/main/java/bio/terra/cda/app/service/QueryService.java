package bio.terra.cda.app.service;

import bio.terra.cda.generated.model.QueryNode;
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
import java.util.UUID;

public class QueryService {

  final BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

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
        jsonData.add(row.get("value").getStringValue());
      }

      return jsonData;
    } catch (InterruptedException e) {
      throw new RuntimeException("Error while getting query results", e);
    }
  }

  public List<String> runQuery(QueryNode queryNode) {
    String query = createQueryFromNode(queryNode);
    // Wrap query so it returns JSON
    String jsonQuery = String.format("SELECT TO_JSON_STRING(t,true) from (%s) as t", query);
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(jsonQuery).setUseLegacySql(true).build();

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    queryJob = runJob(queryJob);

    return getJobResults(queryJob);
  }

  private String createQueryFromNode(QueryNode queryNode) {
    // FIXME need to implement
    return "SELECT X from Y WHERE Z";
  }
}
