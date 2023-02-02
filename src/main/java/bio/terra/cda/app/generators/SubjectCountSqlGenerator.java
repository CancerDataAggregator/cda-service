package bio.terra.cda.app.generators;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    entity = "Subject",
    fieldsToCount = {
      DataSetInfo.FILES_COLUMN,
      "subject_identifier_system",
      "sex",
      "race",
      "ethnicity",
      "cause_of_death"
    })
public class SubjectCountSqlGenerator extends EntityCountSqlGenerator {
  public SubjectCountSqlGenerator(TableSchema tableSchema, String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(tableSchema, qualifiedTable, rootQuery, version, false);
  }

  public SubjectCountSqlGenerator(
          TableSchema tableSchema, String qualifiedTable, Query rootQuery, String version, boolean filesQuery)
      throws IOException {
    super(tableSchema, qualifiedTable, rootQuery, version, filesQuery);
  }
}
