package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Operator;

import java.util.List;

public class BasicOperator extends Operator {
  private BasicOperator parent;
  private String value;
  private List<Operator> operators;

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

  public BasicOperator setOperators(List<Operator> operators) {
    this.operators = operators;
    return this;
  }

  public List<Operator> getOperators() {
    return operators;
  }

  public BasicOperator setValue(String value) {
    this.value = value;
    return this;
  }

  public String getValue() {
    return this.value;
  }
}
