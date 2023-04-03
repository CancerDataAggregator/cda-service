package bio.terra.cda.app.controller;

import bio.terra.cda.common.exception.ErrorReportException;
import bio.terra.cda.generated.model.ErrorReport;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.NoHandlerFoundException;

// This module provides a top-level exception handler for controllers.
// All exceptions that rise through the controllers are caught in this handler.
// It converts the exceptions into standard ErrorReport responses.

@RestControllerAdvice
public class GlobalExceptionHandler {
    private final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    // -- Error Report - one of our exceptions --
    @ExceptionHandler(ErrorReportException.class)
    public ResponseEntity<ErrorReport> errorReportHandler(ErrorReportException ex) {
        if (ex == null) {
            return null;
        }
        return buildErrorReport(ex, ex.getStatusCode(), ex.getCauses());
    }

    // -- validation exceptions - we don't control the exception raised
    @ExceptionHandler({MethodArgumentNotValidException.class, IllegalArgumentException.class,
            NoHandlerFoundException.class})
    public ResponseEntity<ErrorReport> validationExceptionHandler(Exception ex) {
        return buildErrorReport(ex, HttpStatus.BAD_REQUEST, null);
    }

    // -- catchall - log so we can understand what we have missed in the handlers
    // above
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorReport> catchallHandler(Exception ex) {
        logger.error("Exception caught by catchall hander", ex);
        return buildErrorReport(ex, HttpStatus.INTERNAL_SERVER_ERROR, null);
    }

    private ResponseEntity<ErrorReport> buildErrorReport(Throwable ex, HttpStatus statusCode,
            List<String> causes) {
        logger.error("Global exception handler: catch stack", ex);

        List<String> collectCauses = new ArrayList<>();
        for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
            logger.error("   cause: " + cause.toString());
            collectCauses.add(cause.getMessage());
        }

        if (causes == null) {
            causes = collectCauses;
        }

        ErrorReport errorReport = null;
        if (ex != null) {
            errorReport = new ErrorReport().message(ex.getMessage()).statusCode(statusCode.value())
                    .causes(causes);
        }
        return new ResponseEntity<>(errorReport, statusCode);
    }
}
