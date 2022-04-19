package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.generated.model.Query;
import java.util.Arrays;

@QueryOperator(nodeType = Query.NodeTypeEnum.SELECTVALUES)
public class SelectValues extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx)
      throws IllegalArgumentException {
    addUnnests(ctx);
    addSelects(ctx);

    return "";
  }

  @Override
  protected void addUnnests(QueryContext ctx) {
    ctx.addUnnests(Arrays.stream(getValue().split(","))
            .flatMap(select -> SqlUtil.getUnnestsFromParts(ctx.getTable(), select.trim().split("\\."), false)));
  }

  private void addSelects(QueryContext ctx) {
    Arrays.stream(getValue().split(","))
            .forEach(
                    select -> {
                      var parts =
                              Arrays.stream(select.split("\\.")).map(String::trim).toArray(String[]::new);
                      ctx.addSelect(String.format(
                              "%s.%s AS %s",
                              parts.length == 1 ? ctx.getTable() : SqlUtil.getAlias(parts.length - 2, parts),
                              parts[parts.length - 1],
                              String.join("_", parts)));
                    });
  }
}
