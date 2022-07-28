package bio.terra.cda.app.operators;

import bio.terra.cda.app.builders.ParameterBuilder;
import bio.terra.cda.app.builders.QueryFieldBuilder;
import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;

import java.util.Arrays;
import java.util.stream.Collectors;

@QueryOperator(nodeType = {Query.NodeTypeEnum.IN, Query.NodeTypeEnum.NOT_IN})
public class In extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx) {
    String right = ((BasicOperator) getR()).getValue();
    if (!right.contains("[") && !right.contains("("))
      throw new IllegalArgumentException("To use IN you need to add [ or (");

    right = right.substring(1, right.length() - 1);

    ParameterBuilder parameterBuilder = ctx.getParameterBuilder();
    QueryFieldBuilder queryFieldBuilder = ctx.getQueryFieldBuilder();
    QueryField queryField = queryFieldBuilder.fromPath(this.getL().getValue());

    if (right.contains("\"") || right.contains("'")) {
      right = right.substring(1, right.length() - 1);

      String parameterName =
          parameterBuilder.addParameterValue(queryField, right.split("[\"|'](\\s)*,(\\s)*[\"|']"));

      right =
          String.format(
              "(SELECT UPPER(_%2$s) FROM UNNEST(%1$s) as _%2$s)",
              parameterName, parameterName.substring(1));
    } else {
      String parameterName =
          parameterBuilder.addParameterValue(queryField,
                  Arrays.stream(right.split(","))
                          .map(String::trim).toArray());

      right = String.format("UNNEST(%s)", parameterName);
    }

    String left = ((BasicOperator) getL()).buildQuery(ctx);
    return String.format("(%s %s %s)", left, this.getNodeType(), right);
  }
}
