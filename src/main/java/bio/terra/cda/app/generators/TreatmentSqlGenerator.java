package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

@QueryGenerator(
    entity = "treatment",
    hasFiles = false,
    defaultOrderBy = "treatment_id",
    aggregatedFields = {"treatment_identifier_system"},
    aggregatedFieldsSelectString = {
      "json_agg(distinct (treatment_identifier.system, treatment_identifier.field_name, treatment_identifier.value)::system_data) as treatment_identifier"
    })
public class TreatmentSqlGenerator extends SqlGenerator {
  public TreatmentSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
  }
}
