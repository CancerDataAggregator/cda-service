package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.models.Unnest;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import com.google.cloud.bigquery.Field;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class UnnestBuilder {
  private final String table;
  private final String fileTable;
  private final String[] entityParts;
  private final String project;
  private final DataSetInfo dataSetInfo;

  public UnnestBuilder(String table, String fileTable, DataSetInfo dataSetInfo, String[] entityParts, String project) {
    this.table = table;
    this.fileTable = fileTable;
    this.dataSetInfo = dataSetInfo;
    this.entityParts = entityParts;
    this.project = project;
  }

  public Unnest of(
      SqlUtil.JoinType joinType,
      String path,
      String alias,
      boolean isJoin,
      String firstJoinPath,
      String secondJoinPath) {
    return new Unnest(joinType, path, alias, isJoin, firstJoinPath, secondJoinPath);
  }

  public Unnest fileJoin(SqlUtil.JoinType joinType, String joinAlias) {
    return of(
        joinType,
        String.format(SqlUtil.ALIAS_FIELD_FORMAT, project, fileTable),
        fileTable,
        true,
        String.format("%s.id", fileTable),
        joinAlias);
  }

  public Stream<Unnest> fromQueryField(QueryField queryField) {
    return buildUnnestsFromQueryField(queryField, false);
  }

  public Stream<Unnest> fromQueryField(QueryField queryField, boolean includeRepeated) {
    return buildUnnestsFromQueryField(queryField, includeRepeated);
  }

  private Stream<Unnest> buildUnnestsFromQueryField(
      QueryField queryField, boolean includeRepeated) {
    if (queryField.isFileField()) {
      String[] filesParts =
          Stream.concat(
                  Arrays.stream(entityParts),
                  Stream.of(TableSchema.FILES_COLUMN, TableSchema.ID_COLUMN))
              .filter(part -> !part.isEmpty())
              .toArray(String[]::new);
      String filesAlias = SqlUtil.getAlias(filesParts.length - 2, filesParts);

      SqlUtil.JoinType joinType =
          queryField.isFilesQuery() ? SqlUtil.JoinType.INNER : SqlUtil.JoinType.LEFT;
      String[] parts = queryField.isFilesQuery() ? filesParts : entityParts;

      Stream<Unnest> unnestStream =
          Stream.concat(
              fromPartsWithEntityPath(table, filesParts, false, String.join(".", parts)),
              Stream.of(fileJoin(joinType, filesAlias)));

      if (queryField.getParts().length > 1) {
        unnestStream =
            Stream.concat(
                unnestStream,
                fromParts(
                    fileTable,
                    queryField.getParts(),
                    includeRepeated
                        && (queryField.getMode().equals(Field.Mode.REPEATED.toString())),
                    SqlUtil.JoinType.LEFT,
                    String.format("_%s", TableSchema.FILE_PREFIX)));
      }

      return unnestStream;
    } else {
      return fromPartsWithEntityPath(
          table,
          queryField.getParts(),
          includeRepeated && (queryField.getMode().equals(Field.Mode.REPEATED.toString())),
          String.join(".", entityParts));
    }
  }

  public Stream<Unnest> fromPartsWithEntityPath(
      String table, String[] parts, boolean includeLast, String entityPath) {
    return IntStream.range(0, parts.length - (includeLast ? 0 : 1))
        .mapToObj(
            i -> {
              String alias = SqlUtil.getAlias(i, parts);
              String partsSub = Arrays.stream(parts, 0, i + 1).collect(Collectors.joining("."));

              SqlUtil.JoinType joinType =
                  entityPath.startsWith(partsSub) ? SqlUtil.JoinType.INNER : SqlUtil.JoinType.LEFT;

              return i == 0
                  ? new Unnest(
                      joinType, String.format(SqlUtil.ALIAS_FIELD_FORMAT, table, parts[i]), alias)
                  : new Unnest(
                      joinType,
                      String.format(
                          SqlUtil.ALIAS_FIELD_FORMAT, SqlUtil.getAlias(i - 1, parts), parts[i]),
                      alias);
            });
  }

  public Stream<Unnest> fromParts(
      String table, String[] parts, boolean includeLast, SqlUtil.JoinType joinType) {
    return fromParts(table, parts, includeLast, joinType, "");
  }

  public Stream<Unnest> fromParts(
      String table, String[] parts, boolean includeLast, SqlUtil.JoinType joinType, String prefix) {
    return IntStream.range(0, parts.length - (includeLast ? 0 : 1))
        .mapToObj(
            i -> {
              String alias = SqlUtil.getAlias(i, parts);
              alias = prefix.isEmpty() ? alias : String.format("%s%s", prefix, alias);

              return i == 0
                  ? new Unnest(
                      joinType, String.format(SqlUtil.ALIAS_FIELD_FORMAT, table, parts[i]), alias)
                  : new Unnest(
                      joinType,
                      String.format(
                          SqlUtil.ALIAS_FIELD_FORMAT, SqlUtil.getAlias(i - 1, parts), parts[i]),
                      alias);
            });
  }
}
