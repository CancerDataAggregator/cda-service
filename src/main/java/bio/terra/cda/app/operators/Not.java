package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Operator;

@QueryOperator(nodeType = {Operator.NodeTypeEnum.NOT})
public class Not extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx) throws IllegalArgumentException {
    return String.format("%s (%s)", getNodeType(), ((BasicOperator) getL()).buildQuery(ctx));
  }
}
