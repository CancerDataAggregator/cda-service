package bio.terra.cda.app.controller;

//import bio.terra.cda.app.aop.TrackExecutionTime;
import bio.terra.cda.app.aop.TrackExecutionTime;
import bio.terra.cda.app.configuration.ApplicationConfiguration;
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
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.http.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
public class MetaApiController implements MetaApi {
  private final ApplicationConfiguration applicationConfiguration;

  @Autowired public QueryService queryService;

  @Autowired
  public MetaApiController(ApplicationConfiguration applicationConfiguration) {
    this.applicationConfiguration = applicationConfiguration;
  }

  @TrackExecutionTime
  @Override
  public ResponseEntity<SystemStatus> serviceStatus() {
    return ResponseEntity.ok().headers(HeaderUtils.getNoCacheResponseHeader()).body(queryService.postgresCheck());
  }

  // For now, the dataset description is hardcoded. In the future, it will probably be read from a
  // table
  private DatasetDescription createDescription() {
    var dateOfRelease =
        OffsetDateTime.of(LocalDate.of(2024, 3, 21), LocalTime.MIN, ZoneOffset.UTC).toString();
    Model m = new Model();
    m.setVersion("1.0");
    m.setDate(dateOfRelease);
    return new DatasetDescription()
        .addDatasetsItem(
            new DatasetInfo()
                .version(applicationConfiguration.getVersion())
                .source("IDC, PDC, GDC and CDS")
                .date(dateOfRelease))
        .cdaVersion("4.0")
        .notes("CDA MVP release")
        .releaseDate(dateOfRelease)
        .cdaModel(m);
  }

  @Override
  public ResponseEntity<List<DatasetDescription>> allReleaseNotes() {
    return ResponseEntity.ok().headers(HeaderUtils.getNoCacheResponseHeader()).body(Collections.singletonList(createDescription()));
  }

  @Override
  public ResponseEntity<DatasetDescription> latestReleaseNotes() {
    return ResponseEntity.ok().headers(HeaderUtils.getNoCacheResponseHeader()).body(createDescription());
  }

}
