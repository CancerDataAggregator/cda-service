package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    Entity = "ResearchSubject",
    FieldsToCount = {
        TableSchema.FILES_COLUMN,
        TableSchema.SYSTEM_IDENTIFIER,
        "primary_diagnosis_condition",
        "primary_diagnosis_site"
    },
    ExcludedFields = {"Specimen", "Diagnosis"})
public class ResearchSubjectCountSqlGenerator extends EntityCountSqlGenerator {
  public ResearchSubjectCountSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version);
  }
}
