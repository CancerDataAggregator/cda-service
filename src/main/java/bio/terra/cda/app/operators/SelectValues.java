package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.app.util.Unnest;
import bio.terra.cda.generated.model.Query;
import com.google.cloud.bigquery.Field;

import java.util.Arrays;
import java.util.stream.Stream;

@QueryOperator(nodeType = {Query.NodeTypeEnum.SELECTVALUES})
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
                  return !tmp.getMode().equals(Field.Mode.REPEATED.toString());
                })
            .flatMap(
                select -> {
                  var entityParts = ctx.getEntityParts();
                  var isFileField = select.toLowerCase().startsWith("file.");
                  var value = isFileField ? select.substring(select.indexOf(".") + 1) : select;
                  if (isFileField) {
                    String[] filesParts =
                        Stream.concat(Arrays.stream(entityParts), Stream.of(TableSchema.FILES_COLUMN, TableSchema.ID_COLUMN))
                            .filter(part -> !part.isEmpty())
                            .toArray(String[]::new);
                    String filesAlias = SqlUtil.getAlias(filesParts.length - 2, filesParts);

                    return Stream.concat(
                        SqlUtil.getUnnestsFromPartsWithEntityPath(
                            ctx, ctx.getTable(), filesParts, false, String.join(".", filesParts)),
                            Stream.of(
                                    new Unnest(SqlUtil.JoinType.INNER,
                                            String.format("%s.%s", ctx.getProject(), ctx.getFileTable()),
                                            ctx.getFileTable())
                                            .setIsJoin(true)
                                            .setFirstJoinPath(String.format("%s.id", ctx.getFileTable()))
                                            .setSecondJoinPath(filesAlias)));
                  } else {
                    return SqlUtil.getUnnestsFromPartsWithEntityPath(
                        ctx,
                        ctx.getFilesQuery() ? ctx.getFileTable() : ctx.getTable(),
                        SqlUtil.getParts(value.trim()),
                        false,
                        String.join(".", entityParts));
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
                  Arrays.stream(SqlUtil.getParts(value)).map(String::trim).toArray(String[]::new);
              String alias = String.join("_", parts);
              String field =
                  String.format(
                      "%s.%s",
                      parts.length == 1
                          ? isFileField ? ctx.getFileTable() : ctx.getTable()
                          : SqlUtil.getAlias(parts.length - 2, parts),
                      parts[parts.length - 1]);
              ctx.addAlias(
                      alias,
                      parts.length == 1
                          ? String.format(
                              "%s.%s", isFileField ? ctx.getFileTable() : ctx.getTable(), value)
                          : value)
                  .addSelect(String.format("%s AS %s", field, alias));
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
                      Arrays.stream(SqlUtil.getParts(select)).map(String::trim).toArray(String[]::new);
                  var isFileField = select.toLowerCase().startsWith("file.");

                  if (Arrays.asList(parts).contains("identifier")
                      && !parts[parts.length - 1].equals("identifier")) {
                    return String.format("%s.system", SqlUtil.getAlias(parts.length - 2, parts));
                  } else {
                    return String.format(
                        "%s.id",
                        isFileField
                            ? ctx.getFileTable()
                            : SqlUtil.getAlias(parts.length - 2, parts));
                  }
                }));
  }
}
