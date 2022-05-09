package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.generated.model.Query;
import java.util.Arrays;

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
                  var tmp =
                      (ctx.getFilesQuery() ? ctx.getFileTableSchemaMap() : ctx.getTableSchemaMap())
                          .get(select);
                  return !tmp.getMode().equals("REPEATED");
                })
            .flatMap(
                select -> {
                  var isFileField = select.toLowerCase().startsWith("file.");
                  var value = isFileField ? select.substring(select.indexOf(".") + 1) : select;
                  return SqlUtil.getUnnestsFromParts(
                      ctx.getFilesQuery() ? ctx.getFileTable() : ctx.getTable(),
                      value.trim().split("\\."),
                      false);
                }));
  }

  private void addSelects(QueryContext ctx) {
    Arrays.stream(getValue().split(","))
        .map(String::trim)
        .forEach(
            select -> {
              var isFileField = select.toLowerCase().startsWith("file.");
              var value = isFileField ? select.substring(select.indexOf(".") + 1) : select;
              var tmp =
                  (ctx.getFilesQuery() ? ctx.getFileTableSchemaMap() : ctx.getTableSchemaMap())
                      .get(select);
              var parts =
                  Arrays.stream(value.split("\\.")).map(String::trim).toArray(String[]::new);
              ctx.addSelect(
                  String.format(
                      "%s.%s AS %s",
                      parts.length == 1
                          ? ctx.getFilesQuery() ? ctx.getFileTable() : ctx.getTable()
                          : SqlUtil.getAlias(parts.length - 2, parts),
                      parts[parts.length - 1],
                      String.join("_", parts)));
            });
  }

  private void addPartitions(QueryContext ctx) {
    ctx.addPartitions(
        Arrays.stream(getValue().split(","))
            .map(String::trim)
            .filter(
                select -> {
                  var tmp =
                      (ctx.getFilesQuery() ? ctx.getFileTableSchemaMap() : ctx.getTableSchemaMap())
                          .get(select);
                  return select.contains(".") && !tmp.getMode().equals("REPEATED");
                })
            .map(
                select -> {
                  var parts =
                      Arrays.stream(select.split("\\.")).map(String::trim).toArray(String[]::new);

                  var tmp =
                      (ctx.getFilesQuery() ? ctx.getFileTableSchemaMap() : ctx.getTableSchemaMap())
                          .get(select);
                  if (Arrays.asList(parts).contains("identifier")
                      && !parts[parts.length - 1].equals("identifier")) {
                    return String.format("%s.system", SqlUtil.getAlias(parts.length - 2, parts));
                  } else {
                    return String.format("%s.id", SqlUtil.getAlias(parts.length - 2, parts));
                  }
                }));
  }
}
