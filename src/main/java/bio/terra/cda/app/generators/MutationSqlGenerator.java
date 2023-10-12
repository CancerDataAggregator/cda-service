package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

@EntityGeneratorData(entity = "mutation", hasFiles = false, defaultOrderBy = "case_barcode",
    aggregatedFields = {},
    aggregatedFieldsSelectString = {})
public class MutationSqlGenerator extends EntitySqlGenerator {
  public MutationSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
  }
}
