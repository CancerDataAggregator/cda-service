package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;

@QueryOperator(nodeType = Query.NodeTypeEnum.UNQUOTED)
public class Unquoted extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx)
      throws IllegalArgumentException {
    return String.format("%s", getValue());
  }
}
