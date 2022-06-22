package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;
import java.util.Arrays;
import java.util.stream.Collectors;

@QueryOperator(nodeType = {Query.NodeTypeEnum.IN, Query.NodeTypeEnum.NOT_IN})
public class In extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx) {
    String right = ((BasicOperator) getR()).buildQuery(ctx);
    if (!right.contains("[") && !right.contains("("))
      throw new IllegalArgumentException("To use IN you need to add [ or (");

    right = right.substring(1, right.length() - 1);

    if (right.contains("\"") || right.contains("'")) {
      right = right.substring(1, right.length() - 1);

      right =
          Arrays.stream(right.split("[\"|'](\\s)*,(\\s)*[\"|']"))
              .map(value -> String.format("UPPER('%s')", value))
              .collect(Collectors.joining(", "));
    }

    String left = ((BasicOperator) getL()).buildQuery(ctx);
    return String.format("(%s %s (%s))", left, this.getNodeType(), right);
  }
}
