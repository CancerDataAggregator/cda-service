package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

@CountQueryGenerator(
    entity = "researchsubject",
    totalFieldsToCount = {
        "id",
        "file_subject.file_alias",
    },
    groupedFieldsToCount = {
      "researchsubject_identifier_system",
      "primary_diagnosis_condition",
      "primary_diagnosis_site"
    })
public class ResearchSubjectCountSqlGenerator extends EntityCountSqlGenerator {
  public ResearchSubjectCountSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
  }

  public ResearchSubjectCountSqlGenerator(Query rootQuery, boolean filesQuery) {
    super(rootQuery, filesQuery);
  }
}
