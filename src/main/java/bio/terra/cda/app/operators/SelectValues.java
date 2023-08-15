package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;

import java.util.Arrays;

@QueryOperator(nodeType = {Query.NodeTypeEnum.SELECTVALUES})
public class SelectValues extends BasicOperator {

  @Override
  public String buildQuery(QueryContext ctx) throws IllegalArgumentException {
    if (ctx.getIncludeSelect()) {
      addJoins(ctx);
      addSelects(ctx);
    }
    return "";
  }

  @Override
  public void addJoins(QueryContext ctx) {
    String entityTable = ctx.getTable();
    Arrays.stream(getValue().split(","))
        .map(String::trim)
        .map(ctx.getQueryFieldBuilder()::fromPath)
        .filter(field -> !field.getTableName().equals(entityTable))
        .forEach(field -> ctx.addJoins(ctx.getJoinBuilder().getJoinsFromQueryField(entityTable, field)));
  }


  private void addSelects(QueryContext ctx) {
    ctx.addSelects(
        Arrays.stream(getValue().split(","))
            .map(String::trim)
            .map(ctx.getQueryFieldBuilder()::fromPath)
            .map(ctx.getSelectBuilder()::fromQueryField));

    Arrays.stream(getValue().split(","))
        .map(String::trim)
        .map(ctx.getQueryFieldBuilder()::fromPath)
        .forEach(queryField -> ctx.addGroupBy(queryField.getColumn()));
  }

}
