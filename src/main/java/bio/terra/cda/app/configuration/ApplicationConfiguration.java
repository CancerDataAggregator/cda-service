package bio.terra.cda.app.configuration;

import bio.terra.cda.app.operators.QueryModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
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
  private String bqTable;
  private String datasetVersion;

  @Value("${project:default}")
  private String project;

  public String getBqTable() {
    return bqTable;
  }

  public void setBqTable(String bqTable) {
    this.bqTable = bqTable;
  }

  public String getDatasetVersion() {
    return datasetVersion;
  }

  public void setDatasetVersion(String datasetVersion) {
    this.datasetVersion = datasetVersion;
  }

  public String getProject() {
    return project;
  }

  public void setProject(String project) {
    this.project = project;
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
        .registerModule(new JavaTimeModule())
        .registerModule(new QueryModule());
  }

  @Bean("bigQuery")
  public BigQuery bigQuery() {
    return BigQueryOptions.newBuilder().setProjectId(project).build().getService();
  }
}
