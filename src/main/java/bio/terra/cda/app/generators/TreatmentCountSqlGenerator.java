package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    entity = "Treatment",
    fieldsToCount = {"treatment_identifier_system", "treatment_type", "treatment_effect"},
    excludedFields = {})
public class TreatmentCountSqlGenerator extends EntityCountSqlGenerator {
  public TreatmentCountSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version, false);
  }
}
