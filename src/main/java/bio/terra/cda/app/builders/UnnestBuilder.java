package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.ForeignKey;
import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.models.TableRelationship;
import bio.terra.cda.app.models.Unnest;
import bio.terra.cda.app.models.View;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import com.google.cloud.bigquery.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UnnestBuilder {
  private final TableInfo entityTable;
  private final String project;
  private final DataSetInfo dataSetInfo;
  private final QueryFieldBuilder queryFieldBuilder;
  private final ViewListBuilder<View, ViewBuilder> viewListBuilder;

  public UnnestBuilder(
      QueryFieldBuilder queryFieldBuilder,
      ViewListBuilder<View, ViewBuilder> viewListBuilder,
      DataSetInfo dataSetInfo,
      TableInfo entityTable,
      String project) {
    this.queryFieldBuilder = queryFieldBuilder;
    this.viewListBuilder = viewListBuilder;
    this.dataSetInfo = dataSetInfo;
    this.entityTable = entityTable;
    this.project = project;
  }

  public Unnest of(
      SqlUtil.JoinType joinType, String path, String alias, boolean isJoin, String joinPath) {
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
      throw new IllegalArgumentException(
          String.format("No path found to field %s", queryField.getPath()));
    }

    return fromRelationshipPath(
        pathToTable,
        queryField.isFileField() && queryField.isFilesQuery()
            ? SqlUtil.JoinType.INNER
            : SqlUtil.JoinType.LEFT,
        includeRepeated);
  }

  public Stream<Unnest> fromRelationshipPath(
      TableRelationship[] relationships, SqlUtil.JoinType joinType, boolean includeRepeated) {
    Stream<Unnest> unnestStream = Stream.empty();

    if (Objects.isNull(relationships) || relationships.length == 0) {
      return unnestStream;
    }

    Queue<TableRelationship> queue =
        Arrays.stream(relationships).collect(Collectors.toCollection(LinkedList::new));

    TableInfo current = relationships[0].getFromTableInfo();

    while (queue.size() > 0) {
      TableRelationship tableRelationship = queue.remove();

      unnestStream =
          Stream.concat(
              unnestStream,
              fromRelationship(current, tableRelationship, joinType, includeRepeated));

      current = tableRelationship.getDestinationTableInfo();
    }

    return unnestStream;
  }

  public Stream<Unnest> fromRelationship(
      TableInfo tableInfo,
      TableRelationship tableRelationship,
      SqlUtil.JoinType joinType,
      boolean includeRepeated) {
    boolean isJoin =
        tableRelationship.getType().equals(TableRelationship.TableRelationshipTypeEnum.JOIN);

    if (tableRelationship.isParent()) {
      return Stream.empty();
    }

    TableInfo destinationTable = tableRelationship.getDestinationTableInfo();

    if (isJoin) {
      List<ForeignKey> foreignKeyList = tableRelationship.getForeignKeys();
      //       TableRelationship otherWay = destinationTable.getRelationships()
      //               .stream().filter(rel ->
      // rel.getTableInfo().getTableName().equals(tableInfo.getTableName()))
      //               .findFirst().orElseThrow();
      //       List<ForeignKey> targetForeignKeyList = otherWay.getForeignKeys();

      List<String> joinConditions = new ArrayList<String>();

      ForeignKey.ForeignKeyTypeEnum foreignKeyTypeEnum = foreignKeyList.get(0).getType();

      Stream<Unnest> unnestStream = Stream.empty();
      String fieldName = tableRelationship.getField();

      TableSchema.SchemaDefinition schemaDefinition =
          this.dataSetInfo.getSchemaDefinitionByFieldName(fieldName);

      if (Objects.isNull(schemaDefinition)) {
        fieldName =
            tableRelationship.isArray()
                ? tableInfo.getAdjustedTableName()
                : DataSetInfo.getNewNameForDuplicate(this.dataSetInfo.getKnownAliases(), fieldName, tableInfo.getTableName());
      }

      QueryField queryField = this.queryFieldBuilder.fromPath(fieldName);

      unnestStream = Stream.concat(unnestStream, this.fromQueryField(queryField, true));

      String originTableJoin =
          queryField.getMode().equals(Field.Mode.REPEATED.toString())
              ? queryField.getColumnText()
              : String.format("%s.%s", tableInfo.getTableAlias(this.dataSetInfo), queryField.getName());

      String tableString = String.format(
              SqlUtil.ALIAS_FIELD_FORMAT, project, destinationTable.getTableName());

      boolean skip = false;
      boolean added = false;

      for (ForeignKey sourceKey : foreignKeyList) {
        for (QueryField field :
            Arrays.stream(sourceKey.getFields())
                .map(queryFieldBuilder::fromPath)
                .collect(Collectors.toList())) {

          String fieldToJoin =
                  field.getMode().equals(Field.Mode.REPEATED.toString())
                          ? field.getColumnText()
                          : String.format(
                          "%s.%s",
                          tableRelationship.getDestinationTableInfo().getTableAlias(this.dataSetInfo), field.getName());

          if (field.getMode().equals(Field.Mode.REPEATED.toString())) {
            tableString = String.format("%s_%s", destinationTable.getTableAlias(this.dataSetInfo), field.getName());
            String fieldAlias = String.format("%s_flattened", field.getName());
            fieldToJoin = String.format("%s.%s", destinationTable.getTableAlias(this.dataSetInfo), fieldAlias);

            if (this.viewListBuilder.contains(tableString)) {
              skip = true;
            } else {
              added = true;

              ViewBuilder viewBuilder = this.viewListBuilder.getViewBuilder();
              TableInfo repeatedTable = this.dataSetInfo.getTableInfoFromField(field.getName());
              viewBuilder.setViewName(tableString)
                         .setTable(destinationTable)
                         .setIncludeAlias(true)
                         .setViewType(View.ViewType.WITH)
                         .addUnnests(this.fromRelationshipPath(destinationTable.getPathToTable(repeatedTable), SqlUtil.JoinType.LEFT, true));

              Arrays.stream(destinationTable.getSchemaDefinitions()).forEach(definition -> {
                  if (definition.getName().equals(field.getName())) {
                    viewBuilder.addSelect(
                            new SelectBuilder(this.dataSetInfo)
                                    .of(field.getColumnText(), fieldAlias));
                  } else {
                    viewBuilder.addSelect(
                            new SelectBuilder(this.dataSetInfo)
                                    .of(String.format("%s.%s",
                                            destinationTable.getTableAlias(this.dataSetInfo),
                                            definition.getName()), ""));
                  }
              });
              this.viewListBuilder.addView(viewBuilder.build());
            }
          }

          joinConditions.add(String.format("%s = %s", originTableJoin, fieldToJoin));
        }
      }

      return Stream.concat(
          unnestStream,
          !skip || added ? Stream.of(
              new Unnest(
                  joinType,
                  tableString,
                  destinationTable.getTableAlias(this.dataSetInfo),
                  true,
                  String.join(
                      foreignKeyTypeEnum.equals(ForeignKey.ForeignKeyTypeEnum.COMPOSITE_AND)
                          ? " AND "
                          : " OR ",
                      joinConditions)))
              : Stream.empty());
    } else {
      if (!includeRepeated
          && destinationTable.getType().equals(TableInfo.TableInfoTypeEnum.ARRAY)) {
        return Stream.empty();
      }

      return Stream.of(
          new Unnest(
              joinType,
              String.format(
                  SqlUtil.ALIAS_FIELD_FORMAT,
                  tableInfo.getTableAlias(this.dataSetInfo),
                  destinationTable.getTableName()),
              destinationTable.getTableAlias(this.dataSetInfo)));
    }
  }
}
