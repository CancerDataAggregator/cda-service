package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;
import java.util.Objects;

public class BasicOperator extends Query {
  private BasicOperator parent;

  public String buildQuery(QueryContext ctx) {
    return String.format(
        "(%s %s %s)",
        ((BasicOperator) getL()).buildQuery(ctx),
        this.getNodeType(),
        ((BasicOperator) getR()).buildQuery(ctx));
  }

  public String getSQLType(BasicOperator caller, QueryContext ctx) {
    BasicOperator left = (BasicOperator) this.getL();
    BasicOperator right = (BasicOperator) this.getR();
    BasicOperator parent = this.getParent();
    if (Objects.nonNull(left) && !left.equals(caller)) {
      String leftSql = left.getSQLType(this, ctx);
      if (Objects.nonNull(leftSql)) {
        return leftSql;
      }
    }
    if (Objects.nonNull(right) && !right.equals(caller)) {
      String rightSql = right.getSQLType(this, ctx);
      if (Objects.nonNull(rightSql)) {
        return rightSql;
      }
    }
    if (Objects.nonNull(parent) && !parent.equals(caller)) {
      String parentSql = parent.getSQLType(this, ctx);
      if (Objects.nonNull(parentSql)) {
        return parentSql;
      }
    }
    return null;
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
