package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

// TODO - case_barcode may need to be altered
@EntityGeneratorData(entity = "mutation", hasFiles = false, defaultOrderBy = "mutation_integer_id_alias",
    aggregatedFields = {},
    aggregatedFieldsSelectString = {})
public class MutationSqlGenerator extends EntitySqlGenerator {
  public MutationSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
  }
}
