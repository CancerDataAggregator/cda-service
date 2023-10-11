package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

@EntityGeneratorData(entity = "diagnosis", hasFiles = false, defaultOrderBy = "diagnosis_id",
    additionalFields = {"subject_id", "researchsubject_id"},
    aggregatedFields = {"diagnosis_identifier_system"},
    aggregatedFieldsSelectString = {
        "json_agg(distinct (diagnosis_identifier.system, diagnosis_identifier.field_name, diagnosis_identifier.value)::system_data) as diagnosis_identifier"}
)
public class DiagnosisSqlGenerator extends EntitySqlGenerator {
  public DiagnosisSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
  }
}
