package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;

public class BasicOperator extends Query {
  private BasicOperator parent;

  public String buildQuery(QueryContext ctx) {
    return String.format(
        "(%s %s %s)",
        ((BasicOperator) getL()).buildQuery(ctx),
        this.getNodeType(),
        ((BasicOperator) getR()).buildQuery(ctx));
  }

  public BasicOperator setParent(BasicOperator operator) {
    this.parent = operator;
    return this;
  }

  protected BasicOperator getParent() {
    return parent;
  }

  protected void addUnnests(QueryContext ctx) {
    ctx.addUnnests(
        ctx.getUnnestBuilder()
            .fromQueryField(ctx.getQueryFieldBuilder().fromPath(getValue()), true));
  }
}
