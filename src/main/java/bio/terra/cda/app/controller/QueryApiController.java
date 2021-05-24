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
import java.util.Collections;
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

  private ResponseEntity<QueryCreatedData> sendQuery(String querySql, boolean dryRun) {
    var response = new QueryCreatedData().querySql(querySql);
    if (!dryRun) {
      response.queryId(queryService.startQuery(querySql));
    }
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<QueryCreatedData> bulkData(String version) {
    String querySql = "SELECT * FROM " + applicationConfiguration.getBqTable() + "." + version;
    return sendQuery(querySql, false);
  }

  @Override
  public ResponseEntity<QueryCreatedData> sqlQuery(String querySql) {
    return sendQuery(querySql, false);
  }

  @Override
  public ResponseEntity<QueryCreatedData> booleanQuery(
      String version, @Valid Query body, @Valid Boolean dryRun) {

    String querySql =
        QueryTranslator.sql(applicationConfiguration.getBqTable() + "." + version, body);

    return sendQuery(querySql, dryRun);
  }
}
