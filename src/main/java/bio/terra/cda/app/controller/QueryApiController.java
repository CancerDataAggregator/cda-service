package bio.terra.cda.app.controller;

import bio.terra.cda.app.configuration.ApplicationConfiguration;
import bio.terra.cda.app.service.QueryService;
import bio.terra.cda.app.util.QueryTranslator;
import bio.terra.cda.generated.controller.QueryApi;
import bio.terra.cda.generated.model.Query;
import bio.terra.cda.generated.model.QueryResponseData;
import java.util.ArrayList;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
public class QueryApiController implements QueryApi {

  public static final int DEFAULT_LIMIT = 100;

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

  private String createNextUrl(String jobId, String pageToken) {
    var path = webRequest.getHeader("origin");
    return String.format("%s/api/v1/query/%s?page_token=%s", path, jobId, pageToken);
  }

  @Override
  public ResponseEntity<QueryResponseData> query(String id, String pageToken) {
    var result = queryService.getQueryResults(id, pageToken);
    var response =
        new QueryResponseData()
            .result(new ArrayList<>(result.items))
            .totalRowCount(result.totalRowCount)
            .nextUrl(createNextUrl(id, result.pageToken));
    return ResponseEntity.ok(response);
  }

  private ResponseEntity<QueryResponseData> sendQuery(
      @Valid String querySql, @Valid Integer limit, boolean dryRun) {
    var response = new QueryResponseData().querySql(querySql);
    if (!dryRun) {
      var result = queryService.startQuery(querySql, limit);
      response
          .result(result.items)
          .totalRowCount(result.totalRowCount)
          .nextUrl(createNextUrl(result.jobId, result.pageToken));
    }
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<QueryResponseData> bulkData(String version, @Valid Integer limit) {
    String querySql = "SELECT * FROM " + applicationConfiguration.getBqTable() + "." + version;
    return sendQuery(querySql, limit, false);
  }

  @Override
  public ResponseEntity<QueryResponseData> sqlQuery(
      String version, @Valid String querySql, @Valid Integer limit) {
    return sendQuery(querySql, limit, false);
  }

  @Override
  public ResponseEntity<QueryResponseData> booleanQuery(
      String version, @Valid Query body, @Valid Integer limit, @Valid Boolean dryRun) {

    String querySql =
        QueryTranslator.sql(applicationConfiguration.getBqTable() + "." + version, body);

    return sendQuery(querySql, limit, dryRun);
  }
}
