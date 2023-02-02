package bio.terra.cda.app.generators;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    entity = "ResearchSubject",
    fieldsToCount = {
      DataSetInfo.FILES_COLUMN,
      "researchsubject_identifier_system",
      "primary_diagnosis_condition",
      "primary_diagnosis_site"
    })
public class ResearchSubjectCountSqlGenerator extends EntityCountSqlGenerator {
  public ResearchSubjectCountSqlGenerator(TableSchema tableSchema, String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(tableSchema, qualifiedTable, rootQuery, version, false);
  }

  public ResearchSubjectCountSqlGenerator(
          TableSchema tableSchema, String qualifiedTable, Query rootQuery, String version, boolean filesQuery)
      throws IOException {
    super(tableSchema, qualifiedTable, rootQuery, version, filesQuery);
  }
}
