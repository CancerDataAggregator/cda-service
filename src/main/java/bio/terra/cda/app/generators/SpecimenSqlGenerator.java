package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

@QueryGenerator(entity = "specimen", hasFiles = true, defaultOrderBy = "specimen_id",
    additionalFields = {"subject_id", "researchsubject_id"},
    aggregatedFields = {"specimen_identifier_system"},
    aggregatedFieldsSelectString = {
        "json_agg(distinct (specimen_identifier.system, specimen_identifier.field_name, specimen_identifier.value)::system_data) as specimen_identifier"})
public class SpecimenSqlGenerator extends SqlGenerator {
  public SpecimenSqlGenerator(Query rootQuery, boolean filesQuery) {
    super(rootQuery, filesQuery);
  }
}
