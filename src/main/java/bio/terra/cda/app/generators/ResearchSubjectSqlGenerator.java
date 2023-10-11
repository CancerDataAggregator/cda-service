package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

@EntityGeneratorData(entity = "researchsubject", hasFiles = true, defaultOrderBy = "researchsubject_id",
    additionalFields = {"subject_id"},
    aggregatedFields = {"researchsubject_identifier_system"},// "researchsubject_associated_project"},
    aggregatedFieldsSelectString = {
        "json_agg(distinct (researchsubject_identifier.system, researchsubject_identifier.field_name, researchsubject_identifier.value)::system_data) as researchsubject_identifier"})
//        "json_agg(distinct researchsubject_associated_project.associated_project) AS researchsubject_associated_project"
public class ResearchSubjectSqlGenerator extends EntitySqlGenerator {
  public ResearchSubjectSqlGenerator(Query rootQuery, boolean filesQuery) {
    super(rootQuery, filesQuery);
  }
}
