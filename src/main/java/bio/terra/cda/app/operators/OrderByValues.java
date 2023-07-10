package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Operator;
import java.util.Arrays;

@QueryOperator(nodeType = Operator.NodeTypeEnum.ORDERBYVALUES)
public class OrderByValues extends BasicOperator {

  @Override
  public String buildQuery(QueryContext ctx) throws IllegalArgumentException {
    addOrderBys(ctx);
    return "";
  }

  private void addOrderBys(QueryContext ctx) {
    ctx.addOrderBys(
        Arrays.stream(getValue().split(","))
            .map(String::trim)
            .map(ctx.getQueryFieldBuilder()::fromPath)
            .map(ctx.getOrderByBuilder()::fromQueryField));
  }
}
