package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    Entity = "Subject",
    FieldsToCount = {"Files", "identifier.system", "sex", "race", "ethnicity", "cause_of_death"},
    ExcludedFields = {"ResearchSubject"})
public class SubjectCountSqlGenerator extends EntityCountSqlGenerator {
  public SubjectCountSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version);
  }
}
