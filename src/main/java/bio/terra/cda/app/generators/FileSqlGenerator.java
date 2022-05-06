package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@QueryGenerator(
    Entity = "File",
    ExcludedFields = {"ResearchSubject", "Diagnosis", "Specimen"})
public class FileSqlGenerator extends SqlGenerator {
  public FileSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version);
  }
}
