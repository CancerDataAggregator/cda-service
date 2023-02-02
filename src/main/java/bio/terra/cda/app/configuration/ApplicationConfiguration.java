package bio.terra.cda.app.configuration;

import bio.terra.cda.app.operators.QueryModule;
import bio.terra.cda.app.service.StorageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.cache.CacheBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.concurrent.TimeUnit;

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
  public String getProject() {
    return project;
  }
  public void setProject(String project) {
    this.project = project;
  }

  @Value("${storageProject:default}")
  private String storageProject;
  public String getStorageProject() {
    return storageProject;
  }
  public void setStorageProject(String storageProject) {
    this.storageProject = storageProject;
  }

  @Value("${bucket:default}")
  private String bucket;
  public String getBucket() {
    return bucket;
  }
  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  @Value("${schemaDirectory:default}")
  private String schemaDirectory;
  public String getSchemaDirectory() {
    return schemaDirectory;
  }
  public void setSchemaDirectory(String schemaDirectory) {
    this.schemaDirectory = schemaDirectory;
  }

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

  @Bean
  public CacheManager cacheManager() {
    return new ConcurrentMapCacheManager("system-status", "schemas", "dataSetInfos");
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

  @Bean("storageService")
  public StorageService storageService() {
    return StorageService
            .newBuilder()
            .setStorage(
                    StorageOptions
                            .newBuilder()
                            .setProjectId(storageProject)
                            .build()
                            .getService())
            .setBucketOptions(StorageService.BucketOptions
                    .newBuilder()
                    .setBucketName(bucket)
                    .setSchemaDirectory(schemaDirectory)
                    .build())
            .build();
  }
}
