package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

@CountQueryGenerator(
    entity = "specimen",
    totalFieldsToCount = {
        "id",
        "file_specimen.file_id",
    },
    groupedFieldsToCount = {
      "specimen_identifier_system",
      "primary_disease_type",
      "source_material_type",
      "specimen_type"
    })
public class SpecimenCountSqlGenerator extends EntityCountSqlGenerator {
  public SpecimenCountSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
  }

  public SpecimenCountSqlGenerator(Query rootQuery, boolean filesQuery) {
    super(rootQuery, filesQuery);
  }
}
