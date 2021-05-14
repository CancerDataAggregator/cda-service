package bio.terra.cda.app.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

  final BigQuery bigQuery =
      BigQueryOptions.newBuilder().setProjectId("gdc-bq-sample").build().getService();

  private final ObjectMapper objectMapper;

  @Autowired
  public QueryService(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  /**
   * Convert a BQ value to a json node.
   *
   * @param value the value to convert
   * @param field the schema field for the value
   * @return a json node for the value
   */
  @VisibleForTesting
  protected JsonNode valueToJson(FieldValue value, Field field) {
    switch (value.getAttribute()) {
      case RECORD:
        var object = objectMapper.createObjectNode();
        var list = value.getRecordValue();
        var subFields = field.getSubFields();
        for (int i = 0; i < subFields.size(); i++) {
          var subField = subFields.get(i);
          object.set(subField.getName(), valueToJson(list.get(i), subField));
        }
        return object;
      case REPEATED:
        var array = objectMapper.createArrayNode();
        for (FieldValue fieldValue : value.getRepeatedValue()) {
          array.add(valueToJson(fieldValue, field));
        }
        return array;
      case PRIMITIVE:
        if (value.isNull()) {
          return NullNode.instance;
        }
        switch (field.getType().getStandardType()) {
          case NUMERIC:
            return DecimalNode.valueOf(value.getNumericValue());
          case BOOL:
            return BooleanNode.valueOf(value.getBooleanValue());
          default:
            // Primitive types other than boolean and numeric are represented as strings.
            return TextNode.valueOf(value.getStringValue());
        }
      default:
        throw new RuntimeException("Unknown field value type: " + value.getAttribute());
    }
  }

  public QueryResult getQueryResults(String queryId, int offset, int pageSize) {
    final Job job = bigQuery.getJob(queryId);
    if (job == null || !job.exists()) {
      throw new RuntimeException("Unknown query " + queryId);
    }
    if (!job.isDone()) {
      // If the Query is still running, return an empty result.
      return new QueryResult(Collections.emptyList(), null);
    }
    return getJobResults(job, offset, pageSize);
  }

  public static class QueryResult {
    public final List<Object> items;
    public final Long totalRowCount;

    QueryResult(List<JsonNode> items, Long totalRowCount) {
      this.items = new ArrayList<>(items);
      this.totalRowCount = totalRowCount;
    }
  }

  private QueryResult getJobResults(Job queryJob, int offset, int pageSize) {
    var options = new ArrayList<BigQuery.QueryResultsOption>();
    if (offset < 0) {
      throw new RuntimeException("Invalid offset: " + offset);
    }
    options.add(BigQuery.QueryResultsOption.startIndex(offset));
    if (pageSize < 1) {
      throw new RuntimeException("Invalid page size: " + pageSize);
    }
    options.add(BigQuery.QueryResultsOption.pageSize(pageSize));
    try {
      // Get the results.
      TableResult result =
          queryJob.getQueryResults(options.toArray(new BigQuery.QueryResultsOption[0]));
      FieldList fields = result.getSchema().getFields();

      List<JsonNode> jsonData = new ArrayList<>();

      int rowCount = 0;
      for (FieldValueList row : result.iterateAll()) {
        jsonData.add(
            valueToJson(
                FieldValue.of(FieldValue.Attribute.RECORD, row),
                Field.of("root", LegacySQLTypeName.RECORD, fields)));

        // This check is required because pageSize is the number of rows BQ retrieves at a time,
        // not the total number of rows returned by iterateAll(). Without this check, this loop
        // would return all rows in the result table.
        if (++rowCount == pageSize) {
          break;
        }
      }

      logQuery(queryJob, jsonData);

      return new QueryResult(jsonData, result.getTotalRows());
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

  // For now, hardcode the known list of systems. In the future, we will get this from the
  // database itself, as each published dataset will have a list of contributing systems.
  private enum Source {
    GDC,
    PDC;
  }

  /**
   * Traverse the json data and collect the number of systems data present in resultsCount.
   *
   * @param jsonData the data to scan
   * @return For each system, the number of rows in jsonData that have data from that system
   */
  private Map<Source, Integer> generateUsageData(List<JsonNode> jsonData) {
    Map<Source, Integer> resultsCount = new EnumMap<>(Source.class);
    // Each node is a single row in the result.
    for (JsonNode jsonNode : jsonData) {
      List<String> systems = jsonNode.findValuesAsText("system");
      Arrays.stream(Source.values())
          .forEach(
              s -> {
                // Each row can match more than one system, so we have to count them all.
                if (systems.contains(s.name())) {
                  resultsCount.put(s, resultsCount.getOrDefault(s, 0) + 1);
                }
              });
    }
    return resultsCount;
  }

  private void logQuery(Job queryJob, List<JsonNode> jsonData) {
    // Log usage data for this response.
    final Map<Source, Integer> resultsCount = generateUsageData(jsonData);
    var elapsed =
        (queryJob.getStatistics().getEndTime() - queryJob.getStatistics().getStartTime()) / 1000.0F;
    // This cast is safe because we've called getQueryResults() and that would throw if this job
    // wasn't a query.
    var queryConfig = (QueryJobConfiguration) queryJob.getConfiguration();
    var logData = new QueryData(queryConfig.getQuery(), elapsed, resultsCount);
    try {
      logger.info(objectMapper.writeValueAsString(logData));
    } catch (JsonProcessingException e) {
      logger.warn("Error converting object to JSON", e);
    }
  }

  public String startQuery(String query, Long limit) {
    var queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false);
    if (limit != null) {
      queryConfig.setMaxResults(limit);
    }

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig.build()).setJobId(jobId).build());

    return queryJob.getJobId().getJob();
  }
}
