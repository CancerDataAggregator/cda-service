package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@QueryGenerator(entity = "Treatment", hasFiles = false)
public class TreatmentSqlGenerator extends SqlGenerator {
  public TreatmentSqlGenerator(TableSchema tableSchema, String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(tableSchema, qualifiedTable, rootQuery, version, false);
  }
}
