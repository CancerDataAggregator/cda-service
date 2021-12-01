package bio.terra.cda.app.service.exception;

import bio.terra.cda.common.exception.BadRequestException;
import java.util.List;

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
