package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Operator;
import com.google.cloud.bigquery.Field;
import java.util.Arrays;

@QueryOperator(nodeType = {Operator.NodeTypeEnum.SELECTVALUES})
public class SelectValues extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx) throws IllegalArgumentException {
    if (ctx.getIncludeSelect()) {
      addUnnests(ctx);
      addSelects(ctx);
      addPartitions(ctx);
    }
    return "";
  }

  @Override
  public void addUnnests(QueryContext ctx) {
    ctx.addUnnests(
        Arrays.stream(getValue().split(","))
            .map(String::trim)
            .map(ctx.getQueryFieldBuilder()::fromPath)
            .filter(field -> !field.getMode().equals(Field.Mode.REPEATED.toString()))
            .flatMap(field -> ctx.getUnnestBuilder().fromQueryField(field)));
  }

  private void addSelects(QueryContext ctx) {
    ctx.addSelects(
        Arrays.stream(getValue().split(","))
            .map(String::trim)
            .map(ctx.getQueryFieldBuilder()::fromPath)
            .map(ctx.getSelectBuilder()::fromQueryField));
  }

  private void addPartitions(QueryContext ctx) {
    ctx.addPartitions(
        Arrays.stream(getValue().split(","))
            .map(String::trim)
            .map(ctx.getQueryFieldBuilder()::fromPath)
            .map(ctx.getPartitionBuilder()::fromQueryField));
  }
}
