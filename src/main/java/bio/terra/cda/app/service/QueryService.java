package bio.terra.cda.app.service;

import bio.terra.cda.app.service.exception.BadQueryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldList;
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
import java.util.concurrent.TimeUnit;
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

  private static class Results {
    final List<ObjectNode> jsonData = new ArrayList<>();
    /** For each system, the number of rows in jsonData that have data from that system. */
    final Map<Source, Integer> resultsCount = new EnumMap<>(Source.class);
  }

  private Results getJobResults(Job queryJob) {
    try {
      // Get the results.
      TableResult result = queryJob.getQueryResults();
      FieldList fields = result.getSchema().getFields();

      Results results = new Results();

      // Copy all row data to results. For each row, create a Json object to hold the column/value data.
      for (FieldValueList row : result.iterateAll()) {
        ObjectNode rowObject = objectMapper.createObjectNode();
        StringBuilder allFieldValues = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
          rowObject.put(fields.get(i).getName(), row.get(i).getStringValue());
          allFieldValues.append(row.get(i).getStringValue());
        }
        results.jsonData.add(rowObject);

        String allFieldData = allFieldValues.toString();
        Arrays.stream(Source.values())
            .forEach(
                s -> {
                  // Each row can match one or more system.
                  if (s.match(allFieldData)) {
                    results.resultsCount.put(s, results.resultsCount.getOrDefault(s, 0) + 1);
                  }
                });
      }
      return results;
    } catch (InterruptedException e) {
      throw new RuntimeException("Error while getting query results", e);
    }
  }

  private static class QueryData {
    public final String query;
    public final float duration;
    public final Map<Source, Integer> systemUsage;

    QueryData(String query, float duration, Map<Source, Integer> systemUsage) {
      this.query = query;
      this.duration = duration;
      this.systemUsage = systemUsage;
    }
  }

  private static class Timer {
    final long start = System.nanoTime();
    /** @return the time since object creation in seconds */
    float elapsed() {
      return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) / 1000.0F;
    }
  }

  public List<ObjectNode> runQuery(String query) {
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());

    try {
      Timer timer = new Timer();
      Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
      queryJob = runJob(queryJob);
      var jobResults = getJobResults(queryJob);

      try {
        logger.info(
            objectMapper.writeValueAsString(
                new QueryData(query, timer.elapsed(), jobResults.resultsCount)));
      } catch (JsonProcessingException e) {
        logger.warn("Error converting object to JSON", e);
      }
      return jobResults.jsonData;
    } catch (Throwable t) {
      throw new BadQueryException(String.format("Error calling BigQuery: '%s'", query), t);
    }
  }
}
