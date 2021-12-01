package bio.terra.cda.common.exception;

// This base class has data that corresponds to the ErrorReport model generated from
// the OpenAPI yaml. The global exception handler auto-magically converts exceptions
// of this base class into the appropriate ErrorReport REST response.

import org.springframework.http.HttpStatus;

import java.util.List;

public abstract class InternalServerErrorException extends ErrorReportException {
  private static final HttpStatus thisStatus = HttpStatus.INTERNAL_SERVER_ERROR;

  protected InternalServerErrorException(String message) {
    super(message, null, thisStatus);
  }

  protected InternalServerErrorException(String message, Throwable cause) {
    super(message, cause, null, thisStatus);
  }

  protected InternalServerErrorException(Throwable cause) {
    super(null, cause, null, thisStatus);
  }

  protected InternalServerErrorException(String message, List<String> causes) {
    super(message, causes, thisStatus);
  }

  protected InternalServerErrorException(String message, Throwable cause, List<String> causes) {
    super(message, cause, causes, thisStatus);
  }
}
