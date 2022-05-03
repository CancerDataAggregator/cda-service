package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@QueryGenerator(
    Entity = "ResearchSubject",
    ExcludedFields = {"Diagnosis", "Specimen"})
public class ResearchSubjectSqlGenerator extends SqlGenerator {
  public ResearchSubjectSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version);
  }
}
