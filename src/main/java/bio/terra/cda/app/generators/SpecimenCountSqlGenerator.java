package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    Entity = "Specimen",
    FieldsToCount = {
      "Files",
      "identifier.system",
      "primary_disease_type",
      "source_material_type",
      "specimen_type"
    },
    ExcludedFields = {})
public class SpecimenCountSqlGenerator extends EntityCountSqlGenerator {
  public SpecimenCountSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version);
  }
}
