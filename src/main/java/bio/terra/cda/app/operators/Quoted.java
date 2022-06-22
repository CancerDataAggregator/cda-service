package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;

@QueryOperator(nodeType = {Query.NodeTypeEnum.QUOTED})
public class Quoted extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx) {
    return String.format("UPPER('%s')", getValue());
  }
}
