package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    entity = "Diagnosis",
    fieldsToCount = {"diagnosis_identifier_system", "primary_diagnosis", "stage", "grade"})
public class DiagnosisCountSqlGenerator extends EntityCountSqlGenerator {
  public DiagnosisCountSqlGenerator(TableSchema tableSchema, String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(tableSchema, qualifiedTable, rootQuery, version, false);
  }
}
