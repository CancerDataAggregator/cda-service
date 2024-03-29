package bio.terra.cda.common.exception;

// This base class has data that corresponds to the ErrorReport model generated from
// the OpenAPI yaml. The global exception handler auto-magically converts exceptions
// of this base class into the appropriate ErrorReport REST response.

import java.util.List;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.springframework.http.HttpStatus;

public abstract class ErrorReportException extends RuntimeException {
  private final List<String> causes;
  private final HttpStatus statusCode;

  protected ErrorReportException(String message) {
    super(message);
    this.causes = null;
    this.statusCode = HttpStatus.INTERNAL_SERVER_ERROR;
  }

  protected ErrorReportException(String message, Throwable cause) {
    super(message, cause);
    this.causes = null;
    this.statusCode = HttpStatus.INTERNAL_SERVER_ERROR;
  }

  protected ErrorReportException(Throwable cause) {
    super(cause);
    this.causes = null;
    this.statusCode = HttpStatus.INTERNAL_SERVER_ERROR;
  }

  protected ErrorReportException(String message, List<String> causes, HttpStatus statusCode) {
    super(message);
    this.causes = causes;
    this.statusCode = statusCode;
  }

  protected ErrorReportException(
      String message, Throwable cause, List<String> causes, HttpStatus statusCode) {
    super(message, cause);
    this.causes = causes;
    this.statusCode = statusCode;
  }

  public List<String> getCauses() {
    return causes;
  }

  public HttpStatus getStatusCode() {
    return statusCode;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("causes", causes)
        .append("statusCode", statusCode)
        .toString();
  }
}
