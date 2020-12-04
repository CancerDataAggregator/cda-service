package bio.terra.cda.app.controller;

import bio.terra.cda.generated.controller.MetaApi;
import bio.terra.cda.generated.model.SystemStatus;
import bio.terra.cda.generated.model.SystemStatusSystems;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
public class MetaApiController implements MetaApi {

  @Override
  public ResponseEntity<SystemStatus> serviceStatus() {
    HttpStatus httpStatus = HttpStatus.OK;

    SystemStatusSystems otherSystemStatus =
        new SystemStatusSystems().ok(true).addMessagesItem("everything is fine");

    SystemStatus systemStatus =
        new SystemStatus().ok(true).putSystemsItem("BigQuery", otherSystemStatus);

    return new ResponseEntity<>(systemStatus, httpStatus);
  }
}
