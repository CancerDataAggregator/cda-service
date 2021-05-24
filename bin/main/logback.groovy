import org.springframework.cloud.gcp.logging.StackdriverJsonLayout

// LogBack Configuration File
// This file controls how, where, and what gets logged.
// For more information see: https://logback.qos.ch/manual/groovy.html

// Appender that sends to the console
appender("Console-Standard", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%date %-5level [%thread] %logger{36}: %message%n"
    }
}

appender("Console-Stackdriver", ConsoleAppender) {
    encoder(LayoutWrappingEncoder) {
        layout(StackdriverJsonLayout) {
            includeTraceId = true
            includeSpanId = true
        }
    }
}

logger("io.swagger.models.parameters.AbstractSerializableParameter", ERROR)

// root sets the default logging level and appenders
root(INFO, [System.getenv().getOrDefault("LOG_APPENDER", "Console-Stackdriver")])
