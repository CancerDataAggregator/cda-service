package bio.terra.cda.app.controller;

import bio.terra.cda.app.configuration.ApplicationConfiguration;
import bio.terra.cda.app.service.QueryService;
import bio.terra.cda.app.util.QueryTranslator;
import bio.terra.cda.generated.controller.QueryApi;
import bio.terra.cda.generated.model.InlineResponse200;
import bio.terra.cda.generated.model.Query;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
public class QueryApiController implements QueryApi {

  public static final int DEFAULT_LIMIT = 100;
  public static final int DEFAULT_OFFSET = 0;

  private final QueryService queryService;
  private final ApplicationConfiguration applicationConfiguration;


  @Autowired
  public QueryApiController(
      QueryService queryService, ApplicationConfiguration applicationConfiguration) {
    this.queryService = queryService;
    this.applicationConfiguration = applicationConfiguration;
  }

  private ResponseEntity<InlineResponse200> sendQuery(
      @Valid String querySql, @Valid Integer offset, @Valid Integer limit, boolean dryRun) {
    String queryStringWithPagination =
        String.format(
            "%s LIMIT %d OFFSET %d",
            querySql,
            Objects.requireNonNullElse(limit, DEFAULT_LIMIT),
            Objects.requireNonNullElse(offset, DEFAULT_OFFSET));

    var result =
        dryRun ? Collections.emptyList() : queryService.runQuery(queryStringWithPagination);
    var response = new InlineResponse200().result(new ArrayList<>(result)).querySql(querySql);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<InlineResponse200> bulkData(
      String version, @Valid Integer offset, @Valid Integer limit) {
    String querySql = "SELECT * FROM " + applicationConfiguration.getBqTable() + "." + version;
    return sendQuery(querySql, offset, limit, false);
  }

  @Override
  public ResponseEntity<InlineResponse200> sqlQuery(
      String version, @Valid String querySql, @Valid Integer offset, @Valid Integer limit) {
    return sendQuery(querySql, offset, limit, false);
  }

  @Override
  public ResponseEntity<InlineResponse200> booleanQuery(
      String version,
      @Valid Query body,
      @Valid Integer offset,
      @Valid Integer limit,
      @Valid Boolean dryRun) {

    String querySql =
        QueryTranslator.sql(applicationConfiguration.getBqTable() + "." + version, body);

    return sendQuery(querySql, offset, limit, dryRun);
  }
}
