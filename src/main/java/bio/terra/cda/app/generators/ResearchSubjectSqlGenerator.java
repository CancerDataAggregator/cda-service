package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

@EntityGeneratorData(entity = "researchsubject", hasFiles = true, defaultOrderBy = "researchsubject_id",
    aggregatedFields = {"researchsubject_identifier_system"},
    aggregatedFieldsSelectString = {
        "json_agg(distinct (researchsubject_identifier.system, researchsubject_identifier.field_name, researchsubject_identifier.value)::system_data) as researchsubject_identifier"})
public class ResearchSubjectSqlGenerator extends EntitySqlGenerator {
  public ResearchSubjectSqlGenerator(Query rootQuery, boolean filesQuery) {
    super(rootQuery, filesQuery);
  }
}
