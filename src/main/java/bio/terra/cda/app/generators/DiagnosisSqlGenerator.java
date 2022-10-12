package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@QueryGenerator(
    entity = "Diagnosis", hasFiles = false)
public class DiagnosisSqlGenerator extends SqlGenerator {
  public DiagnosisSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version, false);
  }
}
