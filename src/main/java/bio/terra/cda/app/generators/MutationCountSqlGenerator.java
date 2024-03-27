package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    entity = "somatic_mutation",
    totalFieldsToCount = {"subject_alias"},
    groupedFieldsToCount = {
      "chromosome",
      "primary_site",
      "variant_classification",
      "variant_type",
      "mutation_status"
    })
public class MutationCountSqlGenerator extends EntityCountSqlGenerator {
  public MutationCountSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
  }
}
