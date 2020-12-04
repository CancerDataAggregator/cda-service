package bio.terra.cda.app;

import bio.terra.cda.app.configuration.ApplicationConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

public final class StartupInitializer {
  private static final Logger logger = LoggerFactory.getLogger(StartupInitializer.class);

  public static void initialize(ApplicationContext applicationContext) {
    ApplicationConfiguration appConfig =
        (ApplicationConfiguration) applicationContext.getBean("applicationConfiguration");

    // TODO: TEMPLATE: Fill in this method with any other initialization that needs to happen
    //  between the point of having the entire application initialized and
    //  the point of opening the port to start accepting REST requests.
  }
}
