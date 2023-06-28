package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Operator;
import bio.terra.cda.generated.model.Query;

@QueryOperator(nodeType = Operator.NodeTypeEnum.UNQUOTED)
public class Unquoted extends BasicOperator {

  @Override
  public String buildQuery(QueryContext ctx) throws IllegalArgumentException {
    String value = getValue();

    if (value.equalsIgnoreCase("null")) {
      return value;
    }

    var parameterBuilder = ctx.getParameterBuilder();
    return parameterBuilder.addParameterValue(this.getSQLType(this, ctx), value);
  }
}
