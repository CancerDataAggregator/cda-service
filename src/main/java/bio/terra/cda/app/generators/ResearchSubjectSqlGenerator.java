package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@QueryGenerator(entity = "ResearchSubject", hasFiles = true)
public class ResearchSubjectSqlGenerator extends SqlGenerator {
  public ResearchSubjectSqlGenerator(
          TableSchema tableSchema, String qualifiedTable, Query rootQuery, String version, boolean filesQuery)
      throws IOException {
    super(tableSchema, qualifiedTable, rootQuery, version, filesQuery);
  }
}
