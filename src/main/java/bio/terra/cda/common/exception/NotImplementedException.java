package bio.terra.cda.common.exception;

// This base class has data that corresponds to the ErrorReport model generated from
// the OpenAPI yaml. The global exception handler auto-magically converts exceptions
// of this base class into the appropriate ErrorReport REST response.

import java.util.List;
import org.springframework.http.HttpStatus;

public abstract class NotImplementedException extends ErrorReportException {
  private static final HttpStatus thisStatus = HttpStatus.NOT_IMPLEMENTED;

  protected NotImplementedException(String message) {
    super(message, null, thisStatus);
  }

  protected NotImplementedException(String message, Throwable cause) {
    super(message, cause, null, thisStatus);
  }

  protected NotImplementedException(Throwable cause) {
    super(null, cause, null, thisStatus);
  }

  protected NotImplementedException(String message, List<String> causes) {
    super(message, causes, thisStatus);
  }

  protected NotImplementedException(String message, Throwable cause, List<String> causes) {
    super(message, cause, causes, thisStatus);
  }
}
