package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@QueryGenerator(
    entity = "Subject",
    excludedFields = {"ResearchSubject", TableSchema.FILE_PREFIX, TableSchema.FILES_COLUMN})
public class SubjectSqlGenerator extends SqlGenerator {
  public SubjectSqlGenerator(String qualifiedTable, Query rootQuery, String version, boolean filesQuery)
      throws IOException {
    super(qualifiedTable, rootQuery, version, filesQuery);
  }
}
