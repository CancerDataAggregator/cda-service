package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

@EntityGeneratorData(entity = "specimen", hasFiles = true, defaultOrderBy = "specimen_id",
    aggregatedFields = {"specimen_identifier_system"},
    aggregatedFieldsSelectString = {
        "json_agg(distinct (specimen_identifier.system, specimen_identifier.field_name, specimen_identifier.value)::system_data) as specimen_identifier"})
public class SpecimenSqlGenerator extends EntitySqlGenerator {
  public SpecimenSqlGenerator(Query rootQuery, boolean filesQuery) {
    super(rootQuery, filesQuery);
  }
}
