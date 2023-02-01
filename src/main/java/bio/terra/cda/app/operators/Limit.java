package bio.terra.cda.app.operators;

import static java.lang.Integer.parseInt;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;

@QueryOperator(nodeType = {Query.NodeTypeEnum.LIMIT})
public class Limit extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx) {
    ctx.setLimit(parseInt(this.getValue()));
    return ((BasicOperator) getR()).buildQuery(ctx);
  }
}
