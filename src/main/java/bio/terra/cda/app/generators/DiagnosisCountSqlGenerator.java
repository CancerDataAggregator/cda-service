package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

@CountQueryGenerator(
    entity = "diagnosis",
    totalFieldsToCount = {"id"},
    groupedFieldsToCount = {"diagnosis_identifier_system", "primary_diagnosis", "stage", "grade"})
public class DiagnosisCountSqlGenerator extends EntityCountSqlGenerator {
  public DiagnosisCountSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
  }
}
