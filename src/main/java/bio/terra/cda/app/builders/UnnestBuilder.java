package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.ForeignKey;
import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.models.TableRelationship;
import bio.terra.cda.app.models.Unnest;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import com.google.cloud.bigquery.Field;
import nonapi.io.github.classgraph.utils.Join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class UnnestBuilder {
  private final TableInfo entityTable;
  private final String project;
  private final DataSetInfo dataSetInfo;
  private final QueryFieldBuilder queryFieldBuilder;

  public UnnestBuilder(
          QueryFieldBuilder queryFieldBuilder,
          DataSetInfo dataSetInfo,
          TableInfo entityTable,
          String project) {
    this.queryFieldBuilder = queryFieldBuilder;
    this.dataSetInfo = dataSetInfo;
    this.entityTable = entityTable;
    this.project = project;
  }

  public Unnest of(
      SqlUtil.JoinType joinType,
      String path,
      String alias,
      boolean isJoin,
      String joinPath) {
    return new Unnest(joinType, path, alias, isJoin, joinPath);
  }

  public Stream<Unnest> fromQueryField(QueryField queryField) {
    return buildUnnestsFromQueryField(queryField, false);
  }

  public Stream<Unnest> fromQueryField(QueryField queryField, boolean includeRepeated) {
    return buildUnnestsFromQueryField(queryField, includeRepeated);
  }

  private Stream<Unnest> buildUnnestsFromQueryField(
      QueryField queryField, boolean includeRepeated) {
    TableInfo tableInfo = this.dataSetInfo.getTableInfoFromField(queryField.getPath());
    TableRelationship[] pathToTable = this.entityTable.getPathToTable(tableInfo);

    if (Objects.isNull(pathToTable)) {
      throw new IllegalArgumentException(String.format("No path found to field %s", queryField.getPath()));
    }

    return fromRelationshipPath(
            pathToTable,
            queryField.isFileField() && queryField.isFilesQuery()
                    ? SqlUtil.JoinType.INNER : SqlUtil.JoinType.LEFT,
            includeRepeated);
  }

  public Stream<Unnest> fromRelationshipPath(TableRelationship[] relationships, SqlUtil.JoinType joinType, boolean includeRepeated) {
    Stream<Unnest> unnestStream = Stream.empty();

    Queue<TableRelationship> queue = Arrays.stream(relationships)
            .collect(Collectors.toCollection(LinkedList::new));
    TableInfo current = this.entityTable;

    while (queue.size() > 0) {
      TableRelationship tableRelationship = queue.remove();

      unnestStream = Stream.concat(
              unnestStream,
              fromRelationship(current, tableRelationship, joinType, includeRepeated));

      current = tableRelationship.getDestinationTableInfo();
    }

    return unnestStream;
  }

  public Stream<Unnest> fromRelationship(
          TableInfo tableInfo, TableRelationship tableRelationship, SqlUtil.JoinType joinType, boolean includeRepeated) {
    boolean isJoin = tableRelationship.getType().equals(TableRelationship.TableRelationshipTypeEnum.JOIN);
    TableInfo destinationTable = tableRelationship.getDestinationTableInfo();

    if (isJoin) {
       List<ForeignKey> foreignKeyList = tableRelationship.getForeignKeys();
//       TableRelationship otherWay = destinationTable.getRelationships()
//               .stream().filter(rel -> rel.getTableInfo().getTableName().equals(tableInfo.getTableName()))
//               .findFirst().orElseThrow();
//       List<ForeignKey> targetForeignKeyList = otherWay.getForeignKeys();

       List<String> joinConditions = new ArrayList<String>();

       ForeignKey.ForeignKeyTypeEnum foreignKeyTypeEnum = foreignKeyList.get(0).getType();

       Stream<Unnest> unnestStream = Stream.empty();
       String fieldName = tableRelationship.getField();

       TableSchema.SchemaDefinition schemaDefinition = this.dataSetInfo.getSchemaDefinitionByFieldName(fieldName);

       if (Objects.isNull(schemaDefinition)) {
         fieldName = tableRelationship.isArray()
            ? tableInfo.getAdjustedTableName()
            : DataSetInfo.getNewNameForDuplicate(fieldName, tableInfo.getTableName());
       }

      QueryField queryField = this.queryFieldBuilder.fromPath(fieldName);

      unnestStream = Stream.concat(
              unnestStream,
              this.fromQueryField(queryField, true));

      String originTableJoin = queryField.getMode().equals(Field.Mode.REPEATED.toString())
              ? queryField.getColumnText()
              : String.format("%s.%s", tableInfo.getTableAlias(), queryField.getName());

       for (ForeignKey sourceKey : foreignKeyList) {
         for (QueryField field : Arrays.stream(sourceKey.getFields())
                 .map(queryFieldBuilder::fromPath).collect(Collectors.toList())) {

           String fieldToJoin = field.getMode().equals(Field.Mode.REPEATED.toString())
            ? field.getColumnText()
            : String.format("%s.%s", tableRelationship.getDestinationTableInfo().getTableAlias(), field.getName());

           joinConditions.add(
                   String.format(
                           "%s = %s",
                           originTableJoin,
                           fieldToJoin));
         }
       }

       return Stream.concat(
               unnestStream,
               Stream.of(new Unnest(
                 joinType,
                 String.format(SqlUtil.ALIAS_FIELD_FORMAT, project, destinationTable.getTableName()),
                 destinationTable.getTableAlias(),
                 true,
                 String.join(
                         foreignKeyTypeEnum.equals(ForeignKey.ForeignKeyTypeEnum.COMPOSITE_AND)
                                 ? "AND" : "OR",
                         joinConditions))));
    } else {
      if (!includeRepeated && destinationTable.getType().equals(TableInfo.TableInfoTypeEnum.ARRAY)) {
        return Stream.empty();
      }

      return Stream.of(new Unnest(
              joinType,
              String.format(
                      SqlUtil.ALIAS_FIELD_FORMAT,
                      tableInfo.getTableAlias(),
                      destinationTable.getTableName()),
              destinationTable.getTableAlias()));
    }
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
