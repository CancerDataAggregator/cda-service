package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.generated.model.Query;
import java.util.Arrays;
import java.util.stream.Stream;

public class BasicOperator extends Query {
  public String buildQuery(QueryContext ctx) {
    return String.format(
        "(%s %s %s)",
        ((BasicOperator) getL()).buildQuery(ctx),
        this.getNodeType(),
        ((BasicOperator) getR()).buildQuery(ctx));
  }

  protected void addUnnests(QueryContext ctx) {
    try {
      var value = getValue();
      var isFileField = value.toLowerCase().startsWith("file.");
      var schemaMap = isFileField ? ctx.getFileTableSchemaMap() : ctx.getTableSchemaMap();
      if (isFileField) {
        value = value.substring(value.indexOf(".") + 1);
      }

      var tmp = schemaMap.get(value);
      var tmpGetMode = tmp.getMode();
      var parts = value.split("\\.");

      if (isFileField) {
        var entityPath = ctx.getEntityPath();
        var entityParts = entityPath != null
         ? entityPath.split("\\.")
         : new String[0];
        String[] filesParts =
            Stream.concat(Arrays.stream(entityParts), Stream.of("Files", "id"))
                .filter(part -> !part.isEmpty())
                .toArray(String[]::new);
        String filesAlias = SqlUtil.getAlias(filesParts.length - 2, filesParts);

        ctx.addUnnests(SqlUtil.getUnnestsFromParts(ctx.getTable(), filesParts, false));
        ctx.addUnnests(
            Stream.of(
                String.format(
                    " LEFT JOIN %1$s AS %2$s ON %2$s.id = %3$s",
                    String.format("%s.%s", ctx.getProject(), ctx.getFileTable()),
                    ctx.getFileTable(),
                    filesAlias)));
      } else {
        ctx.addUnnests(
            SqlUtil.getUnnestsFromParts(ctx.getTable(), parts, (tmpGetMode.equals("REPEATED"))));
      }
    } catch (NullPointerException e) {
      throw new IllegalArgumentException(
          String.format("Column %s does not exist on table %s", getValue(), ctx.getTable()));
    }
  }
}
