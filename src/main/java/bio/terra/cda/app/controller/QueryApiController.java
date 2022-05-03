package bio.terra.cda.app.controller;

import bio.terra.cda.app.aop.TrackExecutionTime;
import bio.terra.cda.app.configuration.ApplicationConfiguration;
import bio.terra.cda.app.generators.CountsSqlGenerator;
import bio.terra.cda.app.generators.DiagnosisSqlGenerator;
import bio.terra.cda.app.generators.FileSqlGenerator;
import bio.terra.cda.app.generators.ResearchSubjectSqlGenerator;
import bio.terra.cda.app.generators.SpecimenSqlGenerator;
import bio.terra.cda.app.generators.SqlGenerator;
import bio.terra.cda.app.generators.SubjectSqlGenerator;
import bio.terra.cda.app.generators.TreatmentSqlGenerator;
import bio.terra.cda.app.service.QueryService;
import bio.terra.cda.app.service.exception.BadQueryException;
import bio.terra.cda.app.util.NestedColumn;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.controller.QueryApi;
import bio.terra.cda.generated.model.JobStatusData;
import bio.terra.cda.generated.model.Query;
import bio.terra.cda.generated.model.QueryCreatedData;
import bio.terra.cda.generated.model.QueryResponseData;
import com.google.cloud.bigquery.BigQueryException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
public class QueryApiController implements QueryApi {
  private static final Logger logger = LoggerFactory.getLogger(QueryApiController.class);
  private static final String INVALID_DATABASE = "Unable to find schema for that version.";

  private final QueryService queryService;
  private final ApplicationConfiguration applicationConfiguration;
  private final HttpServletRequest webRequest;

  @Autowired
  public QueryApiController(
      QueryService queryService,
      ApplicationConfiguration applicationConfiguration,
      HttpServletRequest webRequest) {
    this.queryService = queryService;
    this.applicationConfiguration = applicationConfiguration;
    this.webRequest = webRequest;
  }

  private String createNextUrl(String jobId, int offset, int limit) {
    var path = String.format("/api/v1/query/%s?offset=%s&limit=%s", jobId, offset, limit);

    try {
      URL baseUrl = new URL(webRequest.getHeader("referer"));
      return new URL(baseUrl.getProtocol(), baseUrl.getHost(), baseUrl.getPort(), path).toString();
    } catch (MalformedURLException e) {
      // Not sure what a good fallback would be here.
      return path;
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryResponseData> query(String id, Integer offset, Integer limit) {
    var result = queryService.getQueryResults(id, offset, limit);
    var response =
        new QueryResponseData()
            .result(Collections.unmodifiableList(result.items))
            .totalRowCount(result.totalRowCount)
            .querySql(result.querySql);
    int nextPage = result.items.size() + limit;
    if (result.totalRowCount == null || nextPage <= result.totalRowCount) {
      response.nextUrl(createNextUrl(id, nextPage, limit));
    }

    return ResponseEntity.ok(response);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<JobStatusData> jobStatus(String id) {
    var response = queryService.getQueryStatusFromJob(id);
    logger.info("****JobStatusController:" + response);
    return ResponseEntity.ok(response);
  }

  private ResponseEntity<QueryCreatedData> sendQuery(String querySql, boolean dryRun) {
    var response = new QueryCreatedData().querySql(querySql);
    //    if (!querySql.contains(applicationConfiguration.getProject())) {
    //      throw new IllegalArgumentException("Your database is outside of the project");
    //    }
    var lowerCaseQuery = querySql.toLowerCase();

    try {
      var supportedSchemas = TableSchema.supportedSchemas();
      var found = false;

      for (String schema : supportedSchemas) {
        if (lowerCaseQuery.contains(schema)) {
          found = true;
          break;
        }
      }

      if (!found) {
        throw new IllegalArgumentException(INVALID_DATABASE);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    }

    if (lowerCaseQuery.contains("create table")
        || lowerCaseQuery.contains("delete from")
        || lowerCaseQuery.contains("drop table")
        || lowerCaseQuery.contains("update")
        || lowerCaseQuery.contains("alter table")) {
      throw new IllegalArgumentException("Those actions are not available in sql");
    }
    if (!dryRun) {
      try {
        response.queryId(queryService.startQuery(querySql));
      } catch (BigQueryException e) {
        throw new BadQueryException("Could not create job");
      }
    }
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> bulkData(String version, String table) {
    String querySql = "SELECT * FROM " + table + "." + version;
    return sendQuery(querySql, false);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> sqlQuery(String querySql) {

    return sendQuery(querySql, false);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> booleanQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      String querySql = new SqlGenerator(table + "." + version, body, version).generate();
      return sendQuery(querySql, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> subjectQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      String querySql = new SubjectSqlGenerator(table + "." + version, body, version).generate();
      return sendQuery(querySql, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> researchSubjectQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      String querySql =
          new ResearchSubjectSqlGenerator(table + "." + version, body, version).generate();
      return sendQuery(querySql, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> specimenQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      String querySql = new SpecimenSqlGenerator(table + "." + version, body, version).generate();
      return sendQuery(querySql, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> diagnosisQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      String querySql = new DiagnosisSqlGenerator(table + "." + version, body, version).generate();
      return sendQuery(querySql, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> treatmentsQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      String querySql = new TreatmentSqlGenerator(table + "." + version, body, version).generate();
      return sendQuery(querySql, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> uniqueValues(
      String version, String body, String system, String table) {
    String tableName;
    if (table == null) {
      tableName = applicationConfiguration.getBqTable() + "." + version;
    } else {
      tableName = table + "." + version;
    }

    NestedColumn nt = NestedColumn.generate(body);
    Set<String> unnestClauses = nt.getUnnestClauses();
    final String whereClause;

    if (system != null && system.length() > 0) {
      NestedColumn whereColumns = NestedColumn.generate("ResearchSubject.identifier.system");
      whereClause = " WHERE " + whereColumns.getColumn() + " = '" + system + "'";
      // add any additional 'where' unnest partials that aren't already included in
      // columns-unnest
      // clauses
      unnestClauses.addAll(whereColumns.getUnnestClauses());
    } else {
      whereClause = "";
    }
    StringBuilder unnestConcat = new StringBuilder();
    unnestClauses.forEach(unnestConcat::append);

    String querySql =
        "SELECT DISTINCT "
            + nt.getColumn()
            + " FROM "
            + tableName
            + unnestConcat
            + whereClause
            + " ORDER BY "
            + nt.getColumn();
    logger.debug("uniqueValues: " + querySql);

    return sendQuery(querySql, false);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> columns(String version, String table) {
    String tableName;
    if (table == null) {
      tableName = applicationConfiguration.getBqTable();
    } else {
      tableName = table;
    }
    String querySql =
        "SELECT field_path FROM "
            + tableName
            + ".INFORMATION_SCHEMA.COLUMN_FIELD_PATHS WHERE table_name = '"
            + version
            + "'";
    logger.debug("columns: " + querySql);

    return sendQuery(querySql, false);
  }

  @Override
  public ResponseEntity<QueryCreatedData> globalCounts(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      String querySql = new CountsSqlGenerator(table + "." + version, body, version).generate();
      return sendQuery(querySql, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> files(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    String querySql = "";
    try {
      querySql = new FileSqlGenerator(table + "." + version, body, version).generate();
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
    return sendQuery(querySql, dryRun);
  }
}
