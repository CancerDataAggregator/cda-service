package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

@QueryGenerator(
    entity = "mutation",
    hasFiles = false,
    defaultOrderBy = "case_barcode",
    aggregatedFields = {},
    aggregatedFieldsSelectString = {})
public class MutationSqlGenerator extends SqlGenerator {
  public MutationSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
  }
}
