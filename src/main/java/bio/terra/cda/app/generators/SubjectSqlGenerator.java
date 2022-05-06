package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@QueryGenerator(
    Entity = "Subject",
    ExcludedFields = {"ResearchSubject", "File", "Files"})
public class SubjectSqlGenerator extends SqlGenerator {
  public SubjectSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version);
  }
}
