package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;

public class BasicOperator extends Query {
  public String buildQuery(QueryContext ctx) {
    return String.format(
        "(%s %s %s)",
        ((BasicOperator) getL()).buildQuery(ctx),
        this.getNodeType(),
        ((BasicOperator) getR()).buildQuery(ctx));
  }

  protected void addUnnests(QueryContext ctx) {
    try {
      ctx.addUnnests(ctx.getUnnestBuilder()
              .fromQueryField(
                      ctx.getQueryFieldBuilder()
                         .fromPath(getValue()),
                      true));
    } catch (NullPointerException e) {
      throw new IllegalArgumentException(
          String.format("Column %s does not exist on table %s", getValue(), ctx.getTable()));
    }
  }
}
