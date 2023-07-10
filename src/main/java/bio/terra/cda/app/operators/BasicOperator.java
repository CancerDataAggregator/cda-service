package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Operator;
import java.util.List;
import java.util.Objects;

public class BasicOperator extends Operator {
  private BasicOperator parent;
  private String value;
  private List<BasicOperator> operators;

  public BasicOperator() {
    QueryOperator operator = this.getClass().getAnnotation(QueryOperator.class);
    if (Objects.nonNull(operator)) {
      this.setNodeType(operator.nodeType());
    }
  }

  public BasicOperator(Operator.NodeTypeEnum nodeType) {
    this.setNodeType(nodeType);
  }

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

  protected void addJoins(QueryContext ctx) {
    ctx.addJoins(
        ctx.getJoinBuilder()
            .getJoinsFromQueryField(
                ctx.getTable(), ctx.getQueryFieldBuilder().fromPath(getValue())));
  }

  public BasicOperator setOperators(List<BasicOperator> operators) {
    this.operators = operators;
    return this;
  }

  public List<BasicOperator> getOperators() {
    return operators;
  }

  public BasicOperator setValue(String value) {
    this.value = value;
    return this;
  }

  public String getValue() {
    return this.value;
  }

  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append(String.format("class %s {\n", this.getClass().getCanonicalName()));
    sb.append("    nodeType: ").append(toIndentedString(this.getNodeType())).append("\n");
    sb.append("    operators: ").append(toIndentedString(this.getOperators())).append("\n");
    sb.append("    value: ").append(toIndentedString(this.getValue())).append("\n");
    sb.append("    modifier: ").append(toIndentedString(this.getModifier())).append("\n");
    sb.append("    l: ").append(toIndentedString(this.getL())).append("\n");
    sb.append("    r: ").append(toIndentedString(this.getR())).append("\n");
    sb.append("}");

    return sb.toString();
  }
}
