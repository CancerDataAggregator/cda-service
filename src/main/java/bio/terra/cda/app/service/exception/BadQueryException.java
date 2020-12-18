package bio.terra.cda.app.service.exception;

import bio.terra.cda.common.exception.BadRequestException;
import java.util.List;

// TODO: TEMPLATE: In general, prefer specific exceptions to general exceptions.
//  The PingService throws this exception. It is caught by the global exception
//  handler and turned into an ErrorReport response.

public class BadQueryException extends BadRequestException {

  public BadQueryException(String message) {
    super(message);
  }

  public BadQueryException(String message, Throwable cause) {
    super(message, cause);
  }

  public BadQueryException(Throwable cause) {
    super(cause);
  }

  public BadQueryException(String message, List<String> causes) {
    super(message, causes);
  }

  public BadQueryException(String message, Throwable cause, List<String> causes) {
    super(message, cause, causes);
  }
}
