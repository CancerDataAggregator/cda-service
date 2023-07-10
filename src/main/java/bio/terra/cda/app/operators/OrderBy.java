package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Operator;
import java.util.Objects;

@QueryOperator(nodeType = Operator.NodeTypeEnum.ORDERBY)
public class OrderBy extends ListOperator {
  @Override
  public String buildQuery(QueryContext ctx) throws IllegalArgumentException {
    String modifier = this.getModifier();
    if (Objects.isNull(modifier)) {
      modifier = "ASC";
    }

    return String.format("%s %s", this.getOperator().buildQuery(ctx), modifier);
  }
}
