package bio.terra.cda.app.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class QueryService {

  private static final Logger logger = LoggerFactory.getLogger(QueryService.class);

  final BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

  @Autowired private ObjectMapper objectMapper;

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
    }
    if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(String.valueOf(queryJob.getStatus().getError()));
    }
    return queryJob;
  }

  public QueryResult getQueryResults(String queryId, String pageToken) {
    final Job job = bigQuery.getJob(queryId);
    if (job != null && job.exists()) {
      // Log results of page query here.
      //      logger.info(
      //              objectMapper.writeValueAsString(
      //                      new QueryData(query, timer.elapsed(), jobResults.resultsCount)));
      return getJobResults(job, pageToken);
    }
    return null;
  }

  // For now, hardcode the known list of systems. In the future, we will get this from the
  // database itself, as each published dataset will have a list of contributing systems.
  private enum Source {
    GDC,
    PDC;

    final String match;

    Source() {
      match = String.format("\"system\": \"%s\"", name());
    }

    boolean match(String s) {
      return s.contains(match);
    }
  }

  public static class QueryResult {
    public final String jobId;
    public final List<Object> items;
    public final String pageToken;
    public final long totalRowCount;

    QueryResult(String jobId, List<Object> items, String pageToken, long totalRowCount) {
      this.jobId = jobId;
      this.items = items;
      this.pageToken = pageToken;
      this.totalRowCount = totalRowCount;
    }
  }

  private QueryResult getJobResults(Job queryJob, String pageToken) {
    final Map<Source, Integer> resultsCount = new EnumMap<>(Source.class);
    var options = new BigQuery.QueryResultsOption[0];
    if (pageToken != null) {
      options =
          new BigQuery.QueryResultsOption[] {BigQuery.QueryResultsOption.pageToken(pageToken)};
    }
    try {
      // Get the results.
      TableResult result = queryJob.getQueryResults(options);

      List<Object> items = new ArrayList<>();

      // Copy all row data to results. Each row is a JSON object.
      for (FieldValueList row : result.iterateAll()) {
        var rowData = row.get(0).getStringValue();
        items.add(rowData);
        Arrays.stream(Source.values())
            .forEach(
                s -> {
                  // Each row can match one or more system.
                  if (s.match(rowData)) {
                    resultsCount.put(s, resultsCount.getOrDefault(s, 0) + 1);
                  }
                });
      }

      return new QueryResult(queryJob.getJobId().getJob(), items, pageToken, result.getTotalRows());
    } catch (InterruptedException e) {
      throw new RuntimeException("Error while getting query results", e);
    }
  }

  /**
   * Start the BQ query and return its job ID.
   *
   * @param query The BQ query to run
   * @return the BQ job ID
   */
  public QueryResult startQuery(String query, Integer limit) {
    // Wrap query so it returns JSON
    String jsonQuery = String.format("SELECT TO_JSON_STRING(t,true) from (%s) as t", query);
    var queryConfig = QueryJobConfiguration.newBuilder(jsonQuery).setUseLegacySql(false);
    if (limit != null) {
      queryConfig.setMaxResults((long) limit);
    }

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig.build()).setJobId(jobId).build());

    return getJobResults(queryJob, null);
  }
}
