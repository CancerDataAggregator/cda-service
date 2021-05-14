package bio.terra.cda.app.controller;

import bio.terra.cda.app.configuration.ApplicationConfiguration;
import bio.terra.cda.app.service.QueryService;
import bio.terra.cda.app.util.QueryTranslator;
import bio.terra.cda.generated.controller.QueryApi;
import bio.terra.cda.generated.model.Query;
import bio.terra.cda.generated.model.QueryCreatedData;
import bio.terra.cda.generated.model.QueryResponseData;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
public class QueryApiController implements QueryApi {

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

  private String createNextUrl(String jobId, Integer offset, Integer pageSize) {
    var path = String.format("/api/v1/query/%s?offset=%s&pageSize=%s", jobId, offset, pageSize);

    URL baseUrl;

    try {
      baseUrl = new URL(webRequest.getHeader("referer"));
      return new URL(baseUrl.getProtocol(), baseUrl.getHost(), baseUrl.getPort(), path).toString();
    } catch (MalformedURLException e) {
      // Not sure what a good fallback would be here.
      return path;
    }
  }

  @Override
  public ResponseEntity<QueryResponseData> query(String id, Integer offset, Integer pageSize) {
    var result = queryService.getQueryResults(id, offset, pageSize);
    var response =
        new QueryResponseData()
            .result(new ArrayList<>(result.items))
            .totalRowCount(result.totalRowCount);
    if (offset + pageSize < result.totalRowCount) {
      response.nextUrl(createNextUrl(id, offset + pageSize, pageSize));
    }
    return ResponseEntity.ok(response);
  }

  private ResponseEntity<QueryCreatedData> sendQuery(
      String querySql, Integer limit, boolean dryRun) {
    var response = new QueryCreatedData().querySql(querySql);
    if (!dryRun) {
      response.queryId(queryService.startQuery(querySql, limit));
    }
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<QueryCreatedData> bulkData(String version, @Valid Integer limit) {
    String querySql = "SELECT * FROM " + applicationConfiguration.getBqTable() + "." + version;
    return sendQuery(querySql, limit, false);
  }

  @Override
  public ResponseEntity<QueryCreatedData> sqlQuery(
      String version, @Valid String querySql, @Valid Integer limit) {
    return sendQuery(querySql, limit, false);
  }

  @Override
  public ResponseEntity<QueryCreatedData> booleanQuery(
      String version, @Valid Query body, @Valid Integer limit, @Valid Boolean dryRun) {

    String querySql =
        QueryTranslator.sql(applicationConfiguration.getBqTable() + "." + version, body);

    return sendQuery(querySql, limit, dryRun);
  }
}
