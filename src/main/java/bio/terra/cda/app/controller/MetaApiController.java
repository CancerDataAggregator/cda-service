package bio.terra.cda.app.controller;

import bio.terra.cda.app.configuration.ApplicationConfiguration;
import bio.terra.cda.app.service.BigQueryClearCache;
import bio.terra.cda.app.service.QueryService;
import bio.terra.cda.generated.controller.MetaApi;
import bio.terra.cda.generated.model.DatasetDescription;
import bio.terra.cda.generated.model.DatasetInfo;
import bio.terra.cda.generated.model.Model;
import bio.terra.cda.generated.model.SystemStatus;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
public class MetaApiController implements MetaApi {
  private static final Logger logger = LoggerFactory.getLogger(MetaApiController.class);
  private final ApplicationConfiguration applicationConfiguration;
  @Autowired public BigQueryClearCache checkStatus;

  @Autowired public QueryService queryService;

  @Autowired
  public MetaApiController(ApplicationConfiguration applicationConfiguration) {
    this.applicationConfiguration = applicationConfiguration;
  }

  @Override
  public ResponseEntity<SystemStatus> serviceStatus() {
    return ResponseEntity.ok(queryService.bigQueryCheck());
  }



  // For now, the dataset description is hardcoded. In the future, it will probably be read from a
  // table in bigquery.
  private DatasetDescription createDescription() {
    var firstOfMarch =
        OffsetDateTime.of(LocalDate.of(2021, 3, 1), LocalTime.MIN, ZoneOffset.UTC).toString();
    return new DatasetDescription()
        .addDatasetsItem(
            new DatasetInfo()
                .version(applicationConfiguration.getDatasetVersion())
                .source("PDC and GDC")
                .date(firstOfMarch))
        .cdaVersion("MVP")
        .notes("CDA MVP release")
        .releaseDate(firstOfMarch)
        .cdaModel(new Model());
  }

  @Override
  public ResponseEntity<List<DatasetDescription>> allReleaseNotes() {
    return ResponseEntity.ok(Collections.singletonList(createDescription()));
  }

  @Override
  public ResponseEntity<DatasetDescription> latestReleaseNotes() {
    return ResponseEntity.ok(createDescription());
  }
}
