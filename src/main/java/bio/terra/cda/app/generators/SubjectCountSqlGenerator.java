package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

@CountQueryGenerator(
    entity = "subject",
    totalFieldsToCount = {
      "id",
      "file_subject.file_id",
    },
    groupedFieldsToCount = {
      "subject_identifier.system",
      "sex",
      "race",
      "ethnicity",
      "cause_of_death"
    })
public class SubjectCountSqlGenerator extends EntityCountSqlGenerator {
  public SubjectCountSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
  }

  public SubjectCountSqlGenerator(Query rootQuery, boolean filesQuery) {
    super(rootQuery, filesQuery);
  }
}
