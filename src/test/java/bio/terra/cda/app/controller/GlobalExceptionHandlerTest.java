package bio.terra.cda.app.controller;

import bio.terra.cda.common.exception.BadRequestException;
import bio.terra.cda.common.exception.ErrorReportException;
import bio.terra.cda.generated.model.ErrorReport;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public class GlobalExceptionHandlerTest {

  @Test
  public void TestErrorReportHandler() throws Exception {
    HttpStatus statusCode = HttpStatus.BAD_REQUEST;
    ErrorReportException erx =
        new BadRequestException("Bad Request") {
          @Override
          public List<String> getCauses() {
            return super.getCauses();
          }
        };

    GlobalExceptionHandler handler = new GlobalExceptionHandler();
    ResponseEntity<ErrorReport> report = handler.errorReportHandler(erx);
    assert (report.getStatusCode() == HttpStatus.BAD_REQUEST);
  }

  @Test
  public void TestValidationExceptionHandler() throws Exception {
    HttpStatus statusCode = HttpStatus.BAD_REQUEST;
    ErrorReportException erx =
        new BadRequestException("Bad Request") {
          @Override
          public List<String> getCauses() {
            return super.getCauses();
          }
        };

    GlobalExceptionHandler handler = new GlobalExceptionHandler();
    ResponseEntity<ErrorReport> report = handler.validationExceptionHandler(erx);
    assert (report.getStatusCode() == HttpStatus.BAD_REQUEST);
  }
}
