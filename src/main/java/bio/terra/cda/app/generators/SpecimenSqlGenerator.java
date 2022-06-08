package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@QueryGenerator(
    Entity = "Specimen",
    ExcludedFields = {TableSchema.FILE_PREFIX, TableSchema.FILES_COLUMN, "derived_from_subject"})
public class SpecimenSqlGenerator extends SqlGenerator {
  public SpecimenSqlGenerator(String qualifiedTable, Query rootQuery, String version, Boolean filesQuery)
      throws IOException {
    super(qualifiedTable, rootQuery, version, filesQuery);
  }
}
