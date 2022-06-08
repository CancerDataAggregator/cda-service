package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    Entity = "Diagnosis",
    FieldsToCount = {TableSchema.SYSTEM_IDENTIFIER, "primary_diagnosis", "stage", "grade"},
    ExcludedFields = {"Treatment"})
public class DiagnosisCountSqlGenerator extends EntityCountSqlGenerator {
  public DiagnosisCountSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version);
  }
}
