package bio.terra.cda.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
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
}
