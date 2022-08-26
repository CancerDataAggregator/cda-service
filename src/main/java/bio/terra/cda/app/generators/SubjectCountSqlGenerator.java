package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    entity = "Subject",
    fieldsToCount = {
      TableSchema.FILES_COLUMN,
      "subject_identifier_system",
      "sex",
      "race",
      "ethnicity",
      "cause_of_death"
    },
    excludedFields = {"ResearchSubject"})
public class SubjectCountSqlGenerator extends EntityCountSqlGenerator {
  public SubjectCountSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version, false);
  }

  public SubjectCountSqlGenerator(
      String qualifiedTable, Query rootQuery, String version, boolean filesQuery)
      throws IOException {
    super(qualifiedTable, rootQuery, version, filesQuery);
  }
}
