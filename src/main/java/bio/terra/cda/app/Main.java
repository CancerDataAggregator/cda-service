package bio.terra.cda.app;

import bio.terra.cda.app.configuration.ApplicationConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@ComponentScan(basePackages = "bio.terra.cda")
@EnableConfigurationProperties(ApplicationConfiguration.class)
@EnableScheduling
@EnableCaching
public class Main {
  public static void main(String[] args) {
    SpringApplication.run(Main.class, args);
  }

  @Profile("staging")
  @Bean
  public String devBean() {
    return "dev";
  }

  @Profile("prod")
  @Bean
  public String prodBean() {
    return "prod";
  }

  @Bean
  public CacheManager cacheManager() {
    return new ConcurrentMapCacheManager("system-status");
  }

  @Bean("objectMapper")
  public ObjectMapper objectMapper() {
    return new ObjectMapper()
        .registerModule(new ParameterNamesModule())
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule());
  }

  // This is a "magic bean": It supplies a method that Spring calls after the application is setup,
  // but before the port is opened for business. That lets us do database migration and stairway
  // initialization on a system that is otherwise fully configured. The rule of thumb is that all
  // bean initialization should avoid database access. If there is additional database work to be
  // done, it should happen inside this method.
  // @Bean
  // public SmartInitializingSingleton postSetupInitialization(ApplicationContext
  // applicationContext) {
  //   return () -> StartupInitializer.initialize(applicationContext);
  // }
}
