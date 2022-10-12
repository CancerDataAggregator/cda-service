package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    entity = "Specimen",
    fieldsToCount = {
      TableSchema.FILES_COLUMN,
      "specimen_identifier_system",
      "primary_disease_type",
      "source_material_type",
      "specimen_type"
    })
public class SpecimenCountSqlGenerator extends EntityCountSqlGenerator {
  public SpecimenCountSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version, false);
  }

  public SpecimenCountSqlGenerator(
      String qualifiedTable, Query rootQuery, String version, boolean filesQuery)
      throws IOException {
    super(qualifiedTable, rootQuery, version, filesQuery);
  }
}
