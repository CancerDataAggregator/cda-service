package bio.terra.cda.app.controller;

import bio.terra.cda.app.aop.TrackExecutionTime;
import bio.terra.cda.app.configuration.ApplicationConfiguration;
import bio.terra.cda.app.generators.CountsSqlGenerator;
import bio.terra.cda.app.generators.DiagnosisCountSqlGenerator;
import bio.terra.cda.app.generators.DiagnosisSqlGenerator;
import bio.terra.cda.app.generators.FileSqlGenerator;
import bio.terra.cda.app.generators.ResearchSubjectCountSqlGenerator;
import bio.terra.cda.app.generators.ResearchSubjectSqlGenerator;
import bio.terra.cda.app.generators.SpecimenCountSqlGenerator;
import bio.terra.cda.app.generators.SpecimenSqlGenerator;
import bio.terra.cda.app.generators.SubjectCountSqlGenerator;
import bio.terra.cda.app.generators.SubjectSqlGenerator;
import bio.terra.cda.app.generators.TreatmentCountSqlGenerator;
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
import com.google.cloud.bigquery.QueryJobConfiguration;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;

import com.google.cloud.bigquery.LegacySQLTypeName;
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

  // region Query Endpoints/Helpers
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
    logger.info("JobStatusController: {}", response);
    return ResponseEntity.ok(response);
  }

  private ResponseEntity<QueryCreatedData> sendQuery(
      QueryJobConfiguration.Builder configBuilder, boolean dryRun) {
    var response = new QueryCreatedData();

    try {
      response = queryService.startQuery(configBuilder, dryRun);
    } catch (BigQueryException e) {
      throw new BadQueryException("Could not create job");
    }

    return new ResponseEntity<>(response, HttpStatus.OK);
  }
  // endregion

  // region Global Queries
  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> bulkData(String version, String table) {
    String querySql = "SELECT * FROM " + table + "." + version;
    QueryJobConfiguration.Builder queryJobBuilder = QueryJobConfiguration.newBuilder(querySql);
    return sendQuery(queryJobBuilder, false);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> booleanQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new SubjectSqlGenerator(table + "." + version, body, version, false).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> uniqueValues(
          String version, String body, String system, String table, Boolean count) {
    String tableName;
    Map<String,TableSchema.SchemaDefinition> tableSchema;

    String myVersion = version;

    var tmpBody = body;
    if (tmpBody
            .toLowerCase()
            .startsWith(String.format("%s.", TableSchema.FILE_PREFIX.toLowerCase()))) {
      tmpBody = tmpBody.replace("File.", "");
      myVersion = myVersion.replace("Subjects", TableSchema.FILES_COLUMN);
    }

    if (table == null) {
      tableName = applicationConfiguration.getBqTable() + "." + myVersion;
    } else {
      tableName = table + "." + myVersion;
    }

    try {
      tableSchema = TableSchema.buildSchemaMap(TableSchema.getSchema(myVersion));
    }catch(IOException e){
      throw new IllegalArgumentException(e.getMessage());
    }

    NestedColumn nt = NestedColumn.generate(tmpBody, tableSchema);
    Set<String> unnestClauses = nt.getUnnestClauses();
    List<String> whereClauses = new ArrayList<>();

    if (tableSchema.get(tmpBody).getType().equals(LegacySQLTypeName.STRING.toString())) {
      whereClauses.add(String.format("IFNULL(%s, '') <> ''", nt.getColumn()));
    } else {
      whereClauses.add(String.format("%s IS NOT NULL", nt.getColumn()));
    }

    if (system != null && system.length() > 0) {
      NestedColumn whereColumns = NestedColumn.generate("ResearchSubject.identifier.system", tableSchema);
      whereClauses.add(whereColumns.getColumn() + " = '" + system + "'");
      // add any additional 'where' unnest partials that aren't already included in
      // columns-unnest
      // clauses
      unnestClauses.addAll(whereColumns.getUnnestClauses());
    }

    StringBuilder unnestConcat = new StringBuilder();
    unnestClauses.forEach(unnestConcat::append);
    var querySql = "";

    if (Boolean.TRUE.equals(count)) {
      querySql =
          "SELECT "
              + nt.getColumn()
              + ","
              + "COUNT("
              + nt.getColumn()
              + ") AS Count "
              + "FROM "
              + tableName
              + unnestConcat
              + " WHERE "
              + String.join(" AND ", whereClauses)
              + " GROUP BY "
              + nt.getColumn()
              + " ORDER BY "
              + nt.getColumn();
    } else {
      querySql =
          "SELECT DISTINCT "
              + nt.getColumn()
              + " FROM "
              + tableName
              + unnestConcat
              + " WHERE "
              + String.join(" AND ", whereClauses)
              + " ORDER BY "
              + nt.getColumn();
    }

    logger.debug("uniqueValues: {}", querySql);

    QueryJobConfiguration.Builder queryJobBuilder = QueryJobConfiguration.newBuilder(querySql);

    return sendQuery(queryJobBuilder, false);
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

    var fileTable = version.replace("Subjects", TableSchema.FILES_COLUMN);

    String querySql =
        String.format(
            "\nWITH Subjects as "
                + "(SELECT\n  field_path\nFROM\n  "
                + "%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS\nWHERE\n  "
                + "table_name = '%s'\n  AND \n  "
                + "NOT CONTAINS_SUBSTR(field_path, \"Files\")\n AND NOT STARTS_WITH(data_type, 'ARRAY<STRUCT')\n),Files AS "
                + "(SELECT\n  \"File.\"|| field_path AS  field_path\nFROM\n  "
                + "%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS\nWHERE\n "
                + " table_name = '%s'\n  AND \n  "
                + "NOT starts_with(field_path, \"Subject\")\n  "
                + "AND \n  NOT starts_with(field_path, "
                + "\"ResearchSubject\")\n  "
                + "AND \n  NOT starts_with(field_path, \"Specimen\")\nAND NOT STARTS_WITH(data_type, 'ARRAY<STRUCT')\n)\n\n\n"
                + "SELECT * FROM Subjects UNION ALL (SELECT * FROM Files)\n\n",
            tableName, version, tableName, fileTable);

    logger.debug("columns: {}", querySql);

    QueryJobConfiguration.Builder queryJobBuilder = QueryJobConfiguration.newBuilder(querySql);

    return sendQuery(queryJobBuilder, false);
  }

  @Override
  public ResponseEntity<QueryCreatedData> globalCounts(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new CountsSqlGenerator(table + "." + version, body, version).generate();
      return sendQuery(configBuilder, dryRun);
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
    QueryJobConfiguration.Builder configBuilder;
    try {
      configBuilder = new FileSqlGenerator(table + "." + version, body, version).generate();
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
    return sendQuery(configBuilder, dryRun);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> fileCountsQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    QueryJobConfiguration.Builder configBuilder;
    try {
      configBuilder =
          new SubjectCountSqlGenerator(table + "." + version, body, version, true).generate();
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
    return sendQuery(configBuilder, dryRun);
  }
  // endregion

  // region Subject Queries
  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> subjectQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new SubjectSqlGenerator(table + "." + version, body, version, false).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> subjectFilesQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new SubjectSqlGenerator(table + "." + version, body, version, true).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> subjectCountsQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new SubjectCountSqlGenerator(table + "." + version, body, version).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> subjectFileCountsQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new SubjectCountSqlGenerator(table + "." + version, body, version, true).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }
  // endregion

  // region ResearchSubject Queries
  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> researchSubjectQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new ResearchSubjectSqlGenerator(table + "." + version, body, version, false).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> researchSubjectFilesQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new ResearchSubjectSqlGenerator(table + "." + version, body, version, true).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> researchSubjectCountsQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new ResearchSubjectCountSqlGenerator(table + "." + version, body, version).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> researchSubjectFileCountsQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new ResearchSubjectCountSqlGenerator(table + "." + version, body, version, true)
              .generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }
  // endregion

  // region Specimen Queries
  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> specimenQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new SpecimenSqlGenerator(table + "." + version, body, version, false).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> specimenFilesQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new SpecimenSqlGenerator(table + "." + version, body, version, true).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> specimenCountsQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new SpecimenCountSqlGenerator(table + "." + version, body, version).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> specimenFileCountsQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new SpecimenCountSqlGenerator(table + "." + version, body, version, true).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }
  // endregion

  // region Diagnosis Queries
  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> diagnosisQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new DiagnosisSqlGenerator(table + "." + version, body, version).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> diagnosisCountsQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new DiagnosisCountSqlGenerator(table + "." + version, body, version).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }
  // endregion

  // region Treatment Queries
  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> treatmentsQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new TreatmentSqlGenerator(table + "." + version, body, version).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryCreatedData> treatmentCountsQuery(
      String version, @Valid Query body, @Valid Boolean dryRun, @Valid String table) {
    try {
      QueryJobConfiguration.Builder configBuilder =
          new TreatmentCountSqlGenerator(table + "." + version, body, version).generate();
      return sendQuery(configBuilder, dryRun);
    } catch (IOException e) {
      throw new IllegalArgumentException(INVALID_DATABASE);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }
  // endregion

}
