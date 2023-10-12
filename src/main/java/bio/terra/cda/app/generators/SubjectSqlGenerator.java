package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

@EntityGeneratorData(
    entity = "subject",
    hasFiles = true,
    defaultOrderBy = "subject_id",
    aggregatedFields = {"subject_identifier_system", "subject_associated_project_associated_project"},
    aggregatedFieldsSelectString = {
        "json_agg(distinct (subject_identifier.system, subject_identifier.field_name, subject_identifier.value)::system_data) as subject_identifier",
        "json_agg(distinct subject_associated_project.associated_project) AS subject_associated_project"})
public class SubjectSqlGenerator extends EntitySqlGenerator {
  public SubjectSqlGenerator(Query rootQuery, boolean filesQuery) {
    super(rootQuery, filesQuery);
  }
}
