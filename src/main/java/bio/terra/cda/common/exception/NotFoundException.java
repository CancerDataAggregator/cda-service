package bio.terra.cda.common.exception;

// This base class has data that corresponds to the ErrorReport model generated from
// the OpenAPI yaml. The global exception handler auto-magically converts exceptions
// of this base class into the appropriate ErrorReport REST response.

import java.util.List;
import org.springframework.http.HttpStatus;

public abstract class NotFoundException extends ErrorReportException {
  private static final HttpStatus thisStatus = HttpStatus.NOT_FOUND;

  protected NotFoundException(String message) {
    super(message, null, thisStatus);
  }

  protected NotFoundException(String message, Throwable cause) {
    super(message, cause, null, thisStatus);
  }

  protected NotFoundException(Throwable cause) {
    super(null, cause, null, thisStatus);
  }

  protected NotFoundException(String message, List<String> causes) {
    super(message, causes, thisStatus);
  }

  protected NotFoundException(String message, Throwable cause, List<String> causes) {
    super(message, cause, causes, thisStatus);
  }
}
