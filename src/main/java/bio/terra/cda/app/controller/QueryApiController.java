package bio.terra.cda.app.controller;

import bio.terra.cda.app.aop.TrackExecutionTime;
import bio.terra.cda.app.builders.JoinBuilder;
import bio.terra.cda.app.builders.QueryFieldBuilder;
import bio.terra.cda.app.configuration.ApplicationConfiguration;
import bio.terra.cda.app.generators.*;
import bio.terra.cda.app.models.*;
import bio.terra.cda.app.service.QueryService;
import bio.terra.cda.app.util.SqlTemplate;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.generated.controller.QueryApi;
import bio.terra.cda.generated.model.ColumnsResponseData;
import bio.terra.cda.generated.model.PagedResponseData;
import bio.terra.cda.generated.model.Query;
import bio.terra.cda.generated.model.QueryResponseData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.*;
import java.util.stream.Collectors;
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

  private final RdbmsSchema rdbmsSchema;

  @Autowired
  public QueryApiController(
      QueryService queryService,
      ApplicationConfiguration applicationConfiguration,
      HttpServletRequest webRequest,
      RdbmsSchema rdbmsSchema) {
    this.queryService = queryService;
    this.applicationConfiguration = applicationConfiguration;
    this.webRequest = webRequest;
    this.rdbmsSchema = rdbmsSchema;
  }

  protected PagedResponseData handleRequest(
      boolean dryRun, SqlGenerator sqlGenerator, Boolean includeCount, Integer offset, Integer limit) {
    return dryRun ? dryRun(sqlGenerator, offset, limit) : runPagedQueryAndReturn(sqlGenerator, includeCount, offset, limit);
  }

  protected QueryResponseData handleRequest(
      boolean dryRun, EntitySqlGenerator sqlGenerator) {
    return dryRun ? dryRun(sqlGenerator) : runAndReturn(sqlGenerator);
  }

  private PagedResponseData checkAndSetNextUrl(PagedResponseData response, String endpoint, int offset, int limit) {
    List<Object> result = response.getResult();
    if (result != null && result.size() == limit) {
      StringBuffer url = webRequest.getRequestURL();
      url.append(String.format("?offset=%s&limit=%s", offset+limit, limit));
      response.setNextUrl(url.toString());
    }
    return response;
  }


  protected QueryResponseData runAndReturn(
      EntitySqlGenerator sqlGenerator) {
    long start = System.currentTimeMillis();
    List<JsonNode> result = queryService.generateAndRunQuery(sqlGenerator);
    String readableSql = "";
    if (sqlGenerator instanceof EntityCountSqlGenerator) {
      readableSql = queryService.getReadableOptimizedCountQuery(sqlGenerator);
    } else {
      readableSql = sqlGenerator.getReadableQuerySql();
    }
    queryService.logQuery(System.currentTimeMillis()-start, readableSql, result, Optional.empty());
    return new QueryResponseData()
            .querySql(readableSql)
            .result(Collections.unmodifiableList(result));
  }

  protected PagedResponseData runPagedQueryAndReturn(SqlGenerator sqlGenerator, Boolean includeCount, Integer offset, Integer limit) {
    long start = System.currentTimeMillis();
    PagedResponseData response = new PagedResponseData();
    Optional<Float> countDuration = Optional.empty();
    if (includeCount) {
      // TODO Use a future for concurrent execution
      response.totalRowCount(queryService.getTotalRowCount(sqlGenerator));
      countDuration = Optional.of((System.currentTimeMillis() - start)/1000.0F);
      start = System.currentTimeMillis();
    }
    List<JsonNode> result = queryService.generateAndRunPagedQuery(sqlGenerator, offset, limit);

//    String readableSql = sqlGenerator.getReadableQuerySql(offset, limit);
    String readableSql = queryService.getReadableOptimizedPagedQuery(sqlGenerator, offset,limit);
    queryService.logQuery(System.currentTimeMillis()-start, readableSql, result, countDuration);
    return
        response
            .querySql(readableSql)
            .result(Collections.unmodifiableList(result));
  }

  protected QueryResponseData dryRun(
      SqlGenerator sqlGenerator
  ) {
    return //new ResponseEntity<>(
        new QueryResponseData()
            .querySql(sqlGenerator.getReadableQuerySql());
//        HttpStatus.OK);

  }

  protected PagedResponseData dryRun(
      SqlGenerator sqlGenerator, Integer offset, Integer limit
  ) {
    return
//    return new ResponseEntity<>(
        new PagedResponseData()
            .querySql(sqlGenerator.getReadableQuerySql(offset, limit));
//        HttpStatus.OK);

  }

  // region Global Queries
  @TrackExecutionTime
  @Override
  public ResponseEntity<PagedResponseData> bulkData(
      @Valid String table, @Valid Boolean includeCount, @Valid Integer offset, @Valid Integer limit) {
    logger.info("executing bulkData query");
    assert(RdbmsSchema.getDataSetInfo().getTableInfo(table) != null);
    String querySql = "SELECT * FROM " + table;
    List<JsonNode> result = queryService.runPagedQuery(querySql, offset, limit);
    return new ResponseEntity<>(
        new PagedResponseData()
            .querySql(querySql)
            .result(Collections.unmodifiableList(result)),
        HttpStatus.OK);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<PagedResponseData> booleanQuery(
      @Valid Query body, @Valid Boolean dryRun, @Valid Boolean includeCount, @Valid Integer offset, @Valid Integer limit) {
    PagedResponseData response = handleRequest(dryRun, new SubjectSqlGenerator(body, false), includeCount, offset, limit);
    checkAndSetNextUrl(response, "boolean-query", offset, limit);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<PagedResponseData> uniqueValues(
      @Valid String body,  @Valid String system,  @Valid Boolean count, @Valid Boolean includeCount, @Valid Integer offset, @Valid Integer limit) {
    if (count == null) {
      count = false;
    }
    PagedResponseData response = handleRequest(false, new QuerySqlGenerator(body, system, count), includeCount, offset, limit);
    checkAndSetNextUrl(response,"unique-values", offset, limit);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<ColumnsResponseData> columns() {
    DataSetInfo dataSetInfo = RdbmsSchema.getDataSetInfo();

    List<ColumnsReturn> columns = dataSetInfo.getColumnsData();
    List<JsonNode> results =
        columns.stream()
            .filter(columnsReturn -> !columnsReturn.getFieldName().contains("integer_id_alias"))
            .map(
                columnsReturn -> {
                  ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
                  objectNode.set("fieldName", TextNode.valueOf(columnsReturn.getFieldName()));
                  objectNode.set("endpoint", TextNode.valueOf(columnsReturn.getEndpoint()));
                  objectNode.set("description", TextNode.valueOf(columnsReturn.getDescription()));
                  objectNode.set("type", TextNode.valueOf(columnsReturn.getType()));
                  objectNode.set("isNullable", BooleanNode.valueOf(columnsReturn.isNullable()));

                  return objectNode;
                })
            .collect(Collectors.toList());

    ColumnsResponseData queryResponseData = new ColumnsResponseData();
    queryResponseData.result(Collections.unmodifiableList(results));

    return new ResponseEntity<>(queryResponseData, HttpStatus.OK);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryResponseData> globalCounts(
      @Valid Query body, @Valid Boolean dryRun) {
    return new ResponseEntity<>(
        handleRequest(dryRun, new CountsSqlGenerator(body)),
        HttpStatus.OK);
  }

  // region Files Queries
    @TrackExecutionTime
    @Override
    public ResponseEntity<PagedResponseData> files(
        @Valid Query body, @Valid Boolean dryRun, @Valid Boolean includeCount, @Valid Integer offset, @Valid Integer limit) {
    PagedResponseData response = handleRequest(dryRun, new FileSqlGenerator(body), includeCount, offset, limit);
    checkAndSetNextUrl(response,"files", offset, limit);
    return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @TrackExecutionTime
    @Override
    public ResponseEntity<QueryResponseData> fileCountsQuery(
        @Valid Query body, @Valid Boolean dryRun) {
      return new ResponseEntity<>(
          handleRequest(dryRun, new SubjectCountSqlGenerator(body, true)),
          HttpStatus.OK);
    }
    // endregion

    // region Subject Queries
    @TrackExecutionTime
    @Override
    public ResponseEntity<PagedResponseData> subjectQuery(
        @Valid Query body, @Valid Boolean dryRun, @Valid Boolean includeCount, @Valid Integer offset, @Valid Integer limit) {
      PagedResponseData response = handleRequest(dryRun, new SubjectSqlGenerator(body, false), includeCount, offset, limit);
      checkAndSetNextUrl(response,"subjects", offset, limit);
      return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @TrackExecutionTime
    @Override
    public ResponseEntity<PagedResponseData> subjectFilesQuery(
        @Valid Query body, @Valid Boolean dryRun, @Valid Boolean includeCount, @Valid Integer offset, @Valid Integer limit) {
    PagedResponseData response =
        handleRequest(dryRun, new SubjectSqlGenerator(body, true), includeCount, offset, limit);
        checkAndSetNextUrl(response,"subjects/files", offset, limit);
      return new ResponseEntity<>(response, HttpStatus.OK);
    }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryResponseData> subjectCountsQuery(
      @Valid Query body, @Valid Boolean dryRun) {
    return new ResponseEntity<>(
        handleRequest(dryRun, new SubjectCountSqlGenerator(body, false)),
        HttpStatus.OK);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryResponseData> subjectFileCountsQuery(
      @Valid Query body, @Valid Boolean dryRun) {
    return new ResponseEntity<>(
        handleRequest(dryRun, new SubjectCountSqlGenerator(body, true)), HttpStatus.OK);
  }
  // endregion



  // region ResearchSubject Queries
  @TrackExecutionTime
  @Override
  public ResponseEntity<PagedResponseData> researchSubjectQuery(
      @Valid Query body, @Valid Boolean dryRun, @Valid Boolean includeCount, @Valid Integer offset, @Valid Integer limit) {
    PagedResponseData response = handleRequest(dryRun, new ResearchSubjectSqlGenerator(body, false), includeCount, offset, limit);
    checkAndSetNextUrl(response,"researchsubjects", offset, limit);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<PagedResponseData> researchSubjectFilesQuery(
      @Valid Query body, @Valid Boolean dryRun, @Valid Boolean includeCount, @Valid Integer offset, @Valid Integer limit) {
    PagedResponseData response = handleRequest(dryRun, new ResearchSubjectSqlGenerator(body, true), includeCount, offset, limit);
    checkAndSetNextUrl(response,"researchsubjects/files", offset, limit);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryResponseData> researchSubjectCountsQuery(
      @Valid Query body, @Valid Boolean dryRun) {
    return new ResponseEntity<>(
        handleRequest(dryRun, new ResearchSubjectCountSqlGenerator(body)),
        HttpStatus.OK);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryResponseData> researchSubjectFileCountsQuery(
      @Valid Query body, @Valid Boolean dryRun) {
    return new ResponseEntity<>(
        handleRequest(dryRun, new ResearchSubjectCountSqlGenerator(body, true)), HttpStatus.OK);
  }
  // endregion

  // region Specimen Queries
  @TrackExecutionTime
  @Override
  public ResponseEntity<PagedResponseData> specimenQuery(
      @Valid Query body, @Valid Boolean dryRun, @Valid Boolean includeCount, @Valid Integer offset, @Valid Integer limit) {
    PagedResponseData response = handleRequest(dryRun, new SpecimenSqlGenerator(body, false), includeCount, offset, limit);
    checkAndSetNextUrl(response,"specimen", offset, limit);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<PagedResponseData> specimenFilesQuery(
      @Valid Query body, @Valid Boolean dryRun, @Valid Boolean includeCount, @Valid Integer offset, @Valid Integer limit) {
    PagedResponseData response = handleRequest(dryRun, new SpecimenSqlGenerator(body, true), includeCount, offset, limit);
    checkAndSetNextUrl(response,"specimen/files", offset, limit);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryResponseData> specimenCountsQuery(
      @Valid Query body, @Valid Boolean dryRun) {
    return new ResponseEntity<>(
        handleRequest(dryRun, new SpecimenCountSqlGenerator(body)),
        HttpStatus.OK);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryResponseData> specimenFileCountsQuery(
      @Valid Query body, @Valid Boolean dryRun) {
    return new ResponseEntity<>(
        handleRequest(dryRun, new SpecimenCountSqlGenerator(body, true)),
        HttpStatus.OK);
  }
  // endregion

  // region Diagnosis Queries
  @TrackExecutionTime
  @Override
  public ResponseEntity<PagedResponseData> diagnosisQuery(
      @Valid Query body, @Valid Boolean dryRun, @Valid Boolean includeCount, @Valid Integer offset, @Valid Integer limit) {
    PagedResponseData response = handleRequest(dryRun, new DiagnosisSqlGenerator(body), includeCount, offset, limit);
    checkAndSetNextUrl(response,"diagnosis", offset, limit);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryResponseData> diagnosisCountsQuery(
      @Valid Query body, @Valid Boolean dryRun) {
    return  new ResponseEntity<>(
        handleRequest(dryRun, new DiagnosisCountSqlGenerator(body)),
        HttpStatus.OK);
  }
  // endregion

  // region Treatment Queries
  @TrackExecutionTime
  @Override
  public ResponseEntity<PagedResponseData> treatmentsQuery(
      @Valid Query body, @Valid Boolean dryRun, @Valid Boolean includeCount, @Valid Integer offset, @Valid Integer limit) {
    PagedResponseData response = handleRequest(dryRun, new TreatmentSqlGenerator(body), includeCount, offset, limit);
    checkAndSetNextUrl(response,"treatments", offset, limit);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryResponseData> treatmentCountsQuery(
      @Valid Query body, @Valid Boolean dryRun) {
    return new ResponseEntity<>(
        handleRequest(dryRun, new TreatmentCountSqlGenerator(body)),
        HttpStatus.OK);
  }
  // endregion

  // region Mutation Queries
  @TrackExecutionTime
  @Override
  public ResponseEntity<PagedResponseData> mutationQuery(
      @Valid Query body, @Valid Boolean dryRun, @Valid Boolean includeCount, @Valid Integer offset, @Valid Integer limit) {
    PagedResponseData response = handleRequest(dryRun, new MutationSqlGenerator(body), includeCount, offset, limit);
    checkAndSetNextUrl(response,"treatments", offset, limit);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<QueryResponseData> mutationCountsQuery(
      @Valid Query body, @Valid Boolean dryRun) {
    return new ResponseEntity<>(
        handleRequest(dryRun, new MutationCountSqlGenerator(body)),
        HttpStatus.OK);
  }
  // endregion

}
