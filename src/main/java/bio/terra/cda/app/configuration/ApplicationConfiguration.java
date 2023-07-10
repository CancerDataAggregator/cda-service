package bio.terra.cda.app.configuration;

import bio.terra.cda.app.models.RdbmsSchema;
import bio.terra.cda.app.operators.QueryModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.util.Arrays;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Component
@Configuration
@EnableTransactionManagement
@ConfigurationProperties(prefix = "cda")
public class ApplicationConfiguration {

  // Configurable properties
  @Value("${version}")
  private String version;

  public String getVersion() {
    return version;
  }

  @Value("${cda.table-precedence}")
  private String[] tablePrecedence;

  public List<String> getTablePrecedence() {
    return Arrays.asList(tablePrecedence);
  }

  //  @Bean
  //  public CacheManager cacheManager() {
  //    return new ConcurrentMapCacheManager("system-status");
  //  }

  @Bean("objectMapper")
  public ObjectMapper objectMapper() {
    return new ObjectMapper()
        .registerModule(new ParameterNamesModule())
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule())
        .registerModule(new QueryModule());
  }

  @Bean()
  public RdbmsSchema rdbmsSchema() {
    return new RdbmsSchema();
  }
}
