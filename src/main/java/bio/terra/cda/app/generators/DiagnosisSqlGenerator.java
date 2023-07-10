package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

@QueryGenerator(
    entity = "diagnosis",
    hasFiles = false,
    defaultOrderBy = "diagnosis_id",
    aggregatedFields = {"diagnosis_identifier_system"},
    aggregatedFieldsSelectString = {
      "json_agg(distinct (diagnosis_identifier.system, diagnosis_identifier.field_name, diagnosis_identifier.value)::system_data) as diagnosis_identifier"
    })
public class DiagnosisSqlGenerator extends SqlGenerator {
  public DiagnosisSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
  }
}
