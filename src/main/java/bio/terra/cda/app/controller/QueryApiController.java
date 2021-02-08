package bio.terra.cda.app.controller;

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
  private final QueryService queryService;

  public static final String CDA_TABLE = "gdc-bq-sample.cda_mvp";

  @Autowired
  public QueryApiController(QueryService queryService) {
    this.queryService = queryService;
  }

  @Override
  public ResponseEntity<InlineResponse200> booleanQuery(
      String version,
      @Valid Query body,
      @Valid Integer offset,
      @Valid Integer limit,
      @Valid Boolean dryRun) {

    String querySql = QueryTranslator.sql(CDA_TABLE + "." + version, body);
    String queryStringWithPagination =
        String.format(
            "%s LIMIT %s OFFSET %s",
            querySql,
            Objects.requireNonNullElse(limit, 100),
            Objects.requireNonNullElse(offset, 0));

    var result =
        dryRun ? Collections.emptyList() : queryService.runQuery(queryStringWithPagination);
    var response = new InlineResponse200().result(new ArrayList<>(result)).querySql(querySql);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }
}
