package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    entity = "mutation",
    totalFieldsToCount = {"id"},
    groupedFieldsToCount = {
      "project_short_name",
      "NCBI_Build",
      "Chromosome",
      "Variant_Type",
      "One_Consequence"
    })
public class MutationCountSqlGenerator extends EntityCountSqlGenerator {
  public MutationCountSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
  }
}
