package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.generated.model.Query;
import java.util.Arrays;
import java.util.stream.Stream;

@QueryOperator(nodeType = Query.NodeTypeEnum.SELECTVALUES)
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
            .filter(
                select -> {
                  var isFileField = select.toLowerCase().startsWith("file.");
                  var value = isFileField ? select.substring(select.indexOf(".") + 1) : select;
                  var tmp =
                      (isFileField ? ctx.getFileTableSchemaMap() : ctx.getTableSchemaMap())
                          .get(value);
                  return !tmp.getMode().equals("REPEATED");
                })
            .flatMap(
                select -> {
                    var entityPath = ctx.getEntityPath();
                    var entityParts = entityPath != null
                            ? entityPath.split("\\.")
                            : new String[0];
                    var isFileField = select.toLowerCase().startsWith("file.");
                    var value = isFileField ? select.substring(select.indexOf(".") + 1) : select;
                    if (isFileField) {
                        String[] filesParts =
                                Stream.concat(Arrays.stream(entityParts), Stream.of("Files", "id"))
                                        .filter(part -> !part.isEmpty())
                                        .toArray(String[]::new);
                        String filesAlias = SqlUtil.getAlias(filesParts.length - 2, filesParts);

                        return Stream.concat(
                                SqlUtil.getUnnestsFromParts(ctx, ctx.getTable(), filesParts, false),
                                Stream.of(String.format(
                                            " LEFT JOIN %1$s AS %2$s ON %2$s.id = %3$s",
                                            String.format("%s.%s", ctx.getProject(), ctx.getFileTable()),
                                            ctx.getFileTable(),
                                            filesAlias)));
                    } else {
                        return SqlUtil.getUnnestsFromParts(ctx,
                                ctx.getFilesQuery() ? ctx.getFileTable() : ctx.getTable(),
                                value.trim().split("\\."),
                                false);
                    }
                }));
  }

  private void addSelects(QueryContext ctx) {
    Arrays.stream(getValue().split(","))
        .map(String::trim)
        .forEach(
            select -> {
              var isFileField = select.toLowerCase().startsWith("file.");
              var value = isFileField ? select.substring(select.indexOf(".") + 1) : select;
              var parts =
                  Arrays.stream(value.split("\\.")).map(String::trim).toArray(String[]::new);
              String alias = String.join("_", parts);
              ctx.addAlias(alias, select)
                 .addSelect(
                      String.format(
                          "%s.%s AS %s",
                          parts.length == 1
                              ? isFileField ? ctx.getFileTable() : ctx.getTable()
                              : SqlUtil.getAlias(parts.length - 2, parts),
                          parts[parts.length - 1],
                          alias));
            });
  }

  private void addPartitions(QueryContext ctx) {
    ctx.addPartitions(
        Arrays.stream(getValue().split(","))
            .map(String::trim)
            .filter(select -> select.contains("."))
            .map(
                select -> {
                  var parts =
                      Arrays.stream(select.split("\\.")).map(String::trim).toArray(String[]::new);
                  var isFileField = select.toLowerCase().startsWith("file.");

                  if (Arrays.asList(parts).contains("identifier")
                      && !parts[parts.length - 1].equals("identifier")) {
                    return String.format("%s.system", SqlUtil.getAlias(parts.length - 2, parts));
                  } else {
                    return String.format("%s.id", isFileField
                            ? ctx.getFileTable() : SqlUtil.getAlias(parts.length - 2, parts));
                  }
                }));
  }
}
