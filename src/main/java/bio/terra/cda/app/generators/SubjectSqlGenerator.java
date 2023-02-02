package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@QueryGenerator(entity = "Subject", hasFiles = true)
public class SubjectSqlGenerator extends SqlGenerator {
  public SubjectSqlGenerator(
          TableSchema tableSchema, String qualifiedTable, Query rootQuery, String version, boolean filesQuery)
      throws IOException {
    super(tableSchema, qualifiedTable, rootQuery, version, filesQuery);
  }
}
