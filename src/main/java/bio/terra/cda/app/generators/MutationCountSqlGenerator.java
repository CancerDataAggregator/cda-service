package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;
import java.io.IOException;

@CountQueryGenerator(
    entity = "Mutation",
    fieldsToCount = {
      "project_short_name",
      "NCBI_Build",
      "Chromosome",
      "Variant_Type",
      "One_Consequence"
    })
public class MutationCountSqlGenerator extends EntityCountSqlGenerator {
  public MutationCountSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version, false);
  }
}
