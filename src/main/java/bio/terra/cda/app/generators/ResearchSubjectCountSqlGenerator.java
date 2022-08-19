package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;
import java.util.Locale;

@CountQueryGenerator(
    entity = "ResearchSubject",
    fieldsToCount = {
      "file_id",
      TableSchema.SYSTEM_IDENTIFIER,
      "primary_diagnosis_condition",
      "primary_diagnosis_site"
    },
    excludedFields = {"Specimen", "Diagnosis"})
public class ResearchSubjectCountSqlGenerator extends EntityCountSqlGenerator {
  public ResearchSubjectCountSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version, false);
  }

  public ResearchSubjectCountSqlGenerator(
      String qualifiedTable, Query rootQuery, String version, boolean filesQuery)
      throws IOException {
    super(qualifiedTable, rootQuery, version, filesQuery);
  }
}
