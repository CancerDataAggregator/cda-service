package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;
import java.util.Arrays;
import java.util.stream.Collectors;

@QueryOperator(nodeType = Query.NodeTypeEnum.IN)
public class In extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx) {
    String right = ((BasicOperator) getR()).buildQuery(ctx);
    if (right.contains("[") || right.contains("(")) {
      right =
          Arrays.stream(right.substring(1, right.length() - 1).replace("\"", "'").split(","))
              .map(
                  value -> {
                    if (value.contains("'")) {
                      return String.format("UPPER(%s)", value);
                    }

                    return value;
                  })
              .collect(Collectors.joining(", "));
    } else {
      throw new IllegalArgumentException("To use IN you need to add [ or (");
    }

    String left = ((BasicOperator) getL()).buildQuery(ctx);
    return String.format("(%s IN (%s))", left, right);
  }
}
