package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    entity = "Diagnosis",
    fieldsToCount = {"diagnosis_identifier_system", "primary_diagnosis", "stage", "grade"},
    excludedFields = {"Treatment"})
public class DiagnosisCountSqlGenerator extends EntityCountSqlGenerator {
  public DiagnosisCountSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version, false);
  }
}
