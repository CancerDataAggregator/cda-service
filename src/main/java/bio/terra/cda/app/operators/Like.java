package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;

@QueryOperator(nodeType = Query.NodeTypeEnum.LIKE)
public class Like extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx) throws IllegalArgumentException {
    String rightValue = getR().getValue();
    String leftValue = ((BasicOperator) getL()).buildQuery(ctx);
    return String.format("%s LIKE UPPER(%s)", leftValue, rightValue);
  }
}
