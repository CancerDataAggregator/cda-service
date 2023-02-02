package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@QueryGenerator(entity = "Diagnosis", hasFiles = false)
public class DiagnosisSqlGenerator extends SqlGenerator {
  public DiagnosisSqlGenerator(TableSchema tableSchema, String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(tableSchema, qualifiedTable, rootQuery, version, false);
  }
}
