package bio.terra.cda.app.operators;

import static java.lang.Integer.parseInt;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;

@QueryOperator(nodeType = {Query.NodeTypeEnum.OFFSET})
public class Offset extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx) {
    ctx.setOffset(parseInt(this.getValue()));
    return ((BasicOperator) getR()).buildQuery(ctx);
  }
}
