package bio.terra.cda.app.operators;

import bio.terra.cda.app.builders.ParameterBuilder;
import bio.terra.cda.app.builders.QueryFieldBuilder;
import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;
import org.apache.logging.log4j.util.Strings;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

    // just set a default
    String parameterName = "param";
    List<String> paramNames = Collections.emptyList();
    // if the array has text, we need to add the UPPER
    if (right.contains("\"") || right.contains("'")) {
//      right = right.substring(1, right.length() - 1);
//    if (queryField.getType().equals("text")) {right.split("[\"|'](\\s)*,(\\s)*[\"|']"));

      List<String> values = Arrays.stream(right.split("(\\s)*,(\\s)*")).map(value -> value.substring(1, value.length() - 1)).collect(Collectors.toList());
      paramNames = values.stream().map(value ->
          parameterBuilder.addParameterValue(queryField, value)).map(name -> String.format("UPPER( %s )", name)).collect(Collectors.toList());
    } else {
      paramNames = Arrays.stream(right.split(",")).map(value ->
          parameterBuilder.addParameterValue(queryField,value.trim())).collect(Collectors.toList());
    }
//    parameterName = parameterBuilder.addParameterValue(queryField, Strings.join(values, ','));
    right =
        String.format(
            "( %s )",
            Strings.join(paramNames, ','));

    String left = ((BasicOperator) getL()).buildQuery(ctx);
    return String.format("(%s %s %s)", left, this.getNodeType(), right);
  }

}
