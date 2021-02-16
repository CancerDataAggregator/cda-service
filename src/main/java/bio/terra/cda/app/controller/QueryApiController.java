package bio.terra.cda.app.controller;

import bio.terra.cda.app.configuration.ApplicationConfiguration;
import bio.terra.cda.app.service.QueryService;
import bio.terra.cda.app.util.QueryTranslator;
import bio.terra.cda.generated.controller.QueryApi;
import bio.terra.cda.generated.model.Query;
import bio.terra.cda.generated.model.QueryResponseData;
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
  private final QueryService queryService;
  private final ApplicationConfiguration applicationConfiguration;

  @Autowired
  public QueryApiController(
      QueryService queryService, ApplicationConfiguration applicationConfiguration) {
    this.queryService = queryService;
    this.applicationConfiguration = applicationConfiguration;
  }

  @Override
  public ResponseEntity<QueryResponseData> booleanQuery(
      String version,
      @Valid Query body,
      @Valid Integer offset,
      @Valid Integer limit,
      @Valid Boolean dryRun) {

    String querySql =
        QueryTranslator.sql(applicationConfiguration.getBqTable() + "." + version, body);
    String queryStringWithPagination =
        String.format(
            "%s LIMIT %s OFFSET %s",
            querySql,
            Objects.requireNonNullElse(limit, 100),
            Objects.requireNonNullElse(offset, 0));

    var result =
        dryRun ? Collections.emptyList() : queryService.runQuery(queryStringWithPagination);
    var response = new QueryResponseData().result(new ArrayList<>(result)).querySql(querySql);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }
}
