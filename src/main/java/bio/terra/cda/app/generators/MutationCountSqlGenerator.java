package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    entity = "mutation",
    totalFieldsToCount = {"id"},
    groupedFieldsToCount = {
      "chromosome",
      "primary_site",
      "variant_class",
      "variant_type",
      "mutation_status"
    })
public class MutationCountSqlGenerator extends EntityCountSqlGenerator {
  public MutationCountSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
  }
}
