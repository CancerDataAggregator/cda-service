package bio.terra.cda.app.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import bio.terra.cda.common.exception.BadRequestException;
import bio.terra.cda.common.exception.ErrorReportException;
import bio.terra.cda.generated.model.ErrorReport;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class GlobalExceptionHandlerTest {

  @Test
  void TestErrorReportHandler() throws Exception {
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
    assertEquals(HttpStatus.BAD_REQUEST, report.getStatusCode());
  }

  @Test
  void TestErrorReportHandlerIncompleteError() throws Exception {
    GlobalExceptionHandler handler = new GlobalExceptionHandler();
    ResponseEntity<ErrorReport> report = handler.errorReportHandler(null);
    assertNull(report);
  }

  @Test
  void TestValidationExceptionHandler() throws Exception {
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
    assertEquals(HttpStatus.BAD_REQUEST, report.getStatusCode());
  }
}
