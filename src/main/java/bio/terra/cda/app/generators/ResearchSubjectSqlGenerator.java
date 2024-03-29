package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@QueryGenerator(entity = "ResearchSubject", hasFiles = true)
public class ResearchSubjectSqlGenerator extends SqlGenerator {
  public ResearchSubjectSqlGenerator(
      String qualifiedTable, Query rootQuery, String version, boolean filesQuery)
      throws IOException {
    super(qualifiedTable, rootQuery, version, filesQuery);
  }
}
