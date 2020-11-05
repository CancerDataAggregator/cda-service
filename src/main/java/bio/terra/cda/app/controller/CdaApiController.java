package bio.terra.cda.app.controller;

import bio.terra.cda.generated.controller.CdaApi;
import bio.terra.cda.service.ping.PingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class CdaApiController implements CdaApi {
  private final PingService pingService;

  @Autowired
  public CdaApiController(PingService pingService) {
    this.pingService = pingService;
  }

  @Override
  public ResponseEntity<String> ping(
      @RequestParam(value = "message", required = false) String message) {
    String result = pingService.computePing(message);
    return new ResponseEntity<>(result, HttpStatus.OK);
  }
}
