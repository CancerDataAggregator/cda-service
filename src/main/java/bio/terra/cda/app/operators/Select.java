package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;

@QueryOperator(nodeType = Query.NodeTypeEnum.SELECT)
public class Select extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx) throws IllegalArgumentException {
    ((BasicOperator) getL()).buildQuery(ctx);
    return ((BasicOperator) getR()).buildQuery(ctx);
  }
}
