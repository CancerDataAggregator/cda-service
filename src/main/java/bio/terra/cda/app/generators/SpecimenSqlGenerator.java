package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@QueryGenerator(
    Entity = "Specimen",
    ExcludedFields = {"File", "Files"})
public class SpecimenSqlGenerator extends SqlGenerator {
  public SpecimenSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version);
  }
}
