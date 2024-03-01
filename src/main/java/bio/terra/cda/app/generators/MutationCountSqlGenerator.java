package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    entity = "somatic_mutation",
    totalFieldsToCount = {"cda_subject_id"},
    groupedFieldsToCount = {
      "project_short_name",
      "ncbi_build",
      "chromosome",
      "variant_type",
      "one_consequence"
    })
public class MutationCountSqlGenerator extends EntityCountSqlGenerator {
  public MutationCountSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
  }
}
