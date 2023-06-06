package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;

import java.util.Arrays;

@QueryOperator(nodeType = Query.NodeTypeEnum.ORDERBYVALUES)
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
