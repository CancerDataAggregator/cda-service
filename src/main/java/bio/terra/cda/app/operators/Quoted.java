package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;

@QueryOperator(nodeType = {Query.NodeTypeEnum.QUOTED})
public class Quoted extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx) {
    String value = getValue();
    // Int check
    if (value.contains("days_to_birth")
        || value.contains("age_at_death")
        || value.contains("age_")) {
      return String.format("'%s'", value);
    }
    return String.format("UPPER('%s')", value);
  }
}
