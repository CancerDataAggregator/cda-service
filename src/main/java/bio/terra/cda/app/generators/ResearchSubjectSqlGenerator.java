package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@QueryGenerator(
    Entity = "ResearchSubject",
    ExcludedFields = {"Diagnosis", "Specimen", TableSchema.FILE_PREFIX, TableSchema.FILES_COLUMN})
public class ResearchSubjectSqlGenerator extends SqlGenerator {
  public ResearchSubjectSqlGenerator(String qualifiedTable, Query rootQuery, String version, Boolean filesQuery)
      throws IOException {
    super(qualifiedTable, rootQuery, version, filesQuery);
  }
}
