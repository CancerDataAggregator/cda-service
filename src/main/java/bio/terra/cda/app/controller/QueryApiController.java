package bio.terra.cda.app.controller;

import bio.terra.cda.app.service.QueryService;
import bio.terra.cda.generated.controller.QueryApi;
import bio.terra.cda.generated.model.InlineResponse200;
import bio.terra.cda.generated.model.Query;
import bio.terra.cda.service.ping.PingService;
import java.util.ArrayList;
import java.util.List;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class QueryApiController implements QueryApi {
  private final PingService pingService;

  @Autowired
  public QueryApiController(PingService pingService) {
    this.pingService = pingService;
  }

  @Override
  public ResponseEntity<String> ping(
      @RequestParam(value = "message", required = false) String message) {
    String result = pingService.computePing(message);
    return new ResponseEntity<>(result, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<InlineResponse200> booleanQuery(
      String version, @Valid Query body, @Valid Integer offset, @Valid Integer limit) {
    QueryService service = new QueryService();
    // FIXME: need try/catch for error handling.
    final List<String> jsonData = service.runQuery(body);

    var response = new InlineResponse200();
    response.setResult(new ArrayList<>(jsonData));
    return new ResponseEntity<>(response, HttpStatus.OK);
  }
}
