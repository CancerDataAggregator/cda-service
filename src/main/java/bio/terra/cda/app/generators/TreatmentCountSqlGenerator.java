package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

@CountQueryGenerator(
    entity = "treatment",
    totalFieldsToCount = {"id"},
    groupedFieldsToCount = {"treatment_identifier_system", "treatment_type", "treatment_effect"})
public class TreatmentCountSqlGenerator extends EntityCountSqlGenerator {
  public TreatmentCountSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
  }
}
