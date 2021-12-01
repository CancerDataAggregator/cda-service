package bio.terra.cda.common.exception;

// This base class has data that corresponds to the ErrorReport model generated from
// the OpenAPI yaml. The global exception handler auto-magically converts exceptions
// of this base class into the appropriate ErrorReport REST response.

import java.util.List;
import org.springframework.http.HttpStatus;

public abstract class UnauthorizedException extends ErrorReportException {
  private static final HttpStatus thisStatus = HttpStatus.UNAUTHORIZED;

  protected UnauthorizedException(String message) {
    super(message, null, thisStatus);
  }

  protected UnauthorizedException(String message, Throwable cause) {
    super(message, cause, null, thisStatus);
  }

  protected UnauthorizedException(Throwable cause) {
    super(null, cause, null, thisStatus);
  }

  protected UnauthorizedException(String message, List<String> causes) {
    super(message, causes, thisStatus);
  }

  protected UnauthorizedException(String message, Throwable cause, List<String> causes) {
    super(message, cause, causes, thisStatus);
  }
}
