package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.OperatorArrayInner;
import java.util.Objects;

public class ListOperator extends OperatorArrayInner {
  private BasicOperator operator;

  public ListOperator() {
    QueryOperator operator = this.getClass().getAnnotation(QueryOperator.class);
    if (Objects.nonNull(operator)) {
      this.setNodeType(NodeTypeEnum.fromValue(operator.nodeType().getValue()));
    }
  }

  public String buildQuery(QueryContext ctx) {
    return " ";
  }

  public ListOperator setOperator(BasicOperator operator) {
    this.operator = operator;
    return this;
  }

  public BasicOperator getOperator() {
    return operator;
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

    sb.append(String.format("class %s {\n", this.getClass().getName()));
    sb.append("    nodeType: ").append(toIndentedString(this.getNodeType())).append("\n");
    sb.append("    operators: ").append(toIndentedString(this.getOperator())).append("\n");
    sb.append("    modifier: ").append(toIndentedString(this.getModifier())).append("\n");
    sb.append("    left: ").append(toIndentedString(this.getL())).append("\n");
    sb.append("    right: ").append(toIndentedString(this.getR())).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
