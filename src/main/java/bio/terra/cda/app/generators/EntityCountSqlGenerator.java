package bio.terra.cda.app.generators;

import bio.terra.cda.app.models.CountByField;
import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.SchemaDefinition;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.service.StorageService;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.QueryUtil;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EntityCountSqlGenerator extends SqlGenerator {
  private List<CountByField> countFields;

  public EntityCountSqlGenerator(
          TableSchema tableSchema, String qualifiedTable, Query rootQuery, String version, boolean filesQuery)
      throws IOException {
    super(tableSchema, qualifiedTable, rootQuery, version, filesQuery);
  }

  @Override
  protected void initializeEntityFields() {
    CountQueryGenerator queryGenerator = this.getClass().getAnnotation(CountQueryGenerator.class);
    this.modularEntity = queryGenerator != null;

    this.entityTable =
        queryGenerator != null
            ? this.dataSetInfo.getTableInfo(queryGenerator.entity())
            : this.dataSetInfo.getTableInfo(version);

    this.filteredFields =
        Arrays.stream(this.entityTable.getSchemaDefinitions())
            .filter(SchemaDefinition::isExcludeFromSelect)
            .map(SchemaDefinition::getName)
            .collect(Collectors.toList());

    //    this.countFields =
    //        queryGenerator != null ? Arrays.asList(queryGenerator.fieldsToCount()) : List.of();
    this.countFields = this.getCountByFields();
  }

  @Override
  protected String sql(
      String tableOrSubClause,
      Query query,
      boolean subQuery,
      boolean hasSubClause,
      boolean ignoreWith)
      throws UncheckedExecutionException, IllegalArgumentException {
    String viewSql =
        super.sql(
            tableOrSubClause, QueryUtil.deSelectifyQuery(query), subQuery, hasSubClause, true);
    String tableAlias = "flattened_result";
    String withStatement = "";
    if (this.viewListBuilder.hasAny() && !ignoreWith) {
      withStatement = String.format("%s, %s as (%s)", getWithStatement(), tableAlias, viewSql);
    } else {
      withStatement = String.format("WITH %s as (%s)", tableAlias, viewSql);
    }

    return subQuery
        ? viewSql
        : String.format("%s select %s", withStatement, getCountSelects(tableAlias));
  }

  protected String getCountSelects(String tableAlias) {
    String formatString =
        "(select ARRAY(select as STRUCT %1$s, count(distinct %2$s) as count "
            + "from %3$s "
            + "group by %1$s)) as %4$s";

    TableInfo table = this.entityTable;
    if (this.filesQuery) {
      table = this.dataSetInfo.getTableInfo(DataSetInfo.FILE_PREFIX);
    }
    String partitionKeyField = table.getPartitionKey();
    if (Objects.isNull(this.dataSetInfo.getSchemaDefinitionByFieldName(table.getPartitionKey()))) {
      partitionKeyField =
          DataSetInfo.getNewNameForDuplicate(
              this.dataSetInfo.getKnownAliases(), table.getPartitionKey(), table.getTableName());
    }

    String finalPartitionKeyField = partitionKeyField;

    return this.countFields.stream()
        .distinct()
        .map(
            countByField -> {
              TableInfo tableToUse = countByField.getTableInfo();

              String fieldToUse = countByField.getField();

              if (Objects.isNull(
                  this.dataSetInfo.getSchemaDefinitionByFieldName(countByField.getField()))) {
                fieldToUse =
                    DataSetInfo.getNewNameForDuplicate(
                        this.dataSetInfo.getKnownAliases(),
                        countByField.getField(),
                        tableToUse.getAdjustedTableName());
              }

              tableToUse = this.dataSetInfo.getTableInfoFromField(fieldToUse);
              if (tableToUse.getType().equals(TableInfo.TableInfoTypeEnum.ARRAY)) {
                fieldToUse = tableToUse.getPartitionKeyAlias(this.dataSetInfo);
              }

              String alias =
                  Objects.nonNull(countByField.getAlias()) ? countByField.getAlias() : fieldToUse;

              return countByField.getType().equals(CountByField.CountByTypeEnum.TOTAL)
                  ? String.format(
                      "(SELECT COUNT(DISTINCT %s) from %s) as %s", fieldToUse, tableAlias, alias)
                  : String.format(
                      formatString, fieldToUse, finalPartitionKeyField, tableAlias, alias);
            })
        .collect(Collectors.joining(", "));
  }

  @Override
  protected Stream<String> getSelectsFromEntity(
      QueryContext ctx, String prefix, boolean skipExcludes) {
    return this.countFields.stream()
        .map(
            countByField -> {
              TableInfo tableToUse = countByField.getTableInfo();

              String fieldToUse = countByField.getField();

              if (Objects.isNull(
                  this.dataSetInfo.getSchemaDefinitionByFieldName(countByField.getField()))) {
                fieldToUse =
                    DataSetInfo.getNewNameForDuplicate(
                        this.dataSetInfo.getKnownAliases(),
                        countByField.getField(),
                        tableToUse.getAdjustedTableName());
              }

              SchemaDefinition definition =
                  this.dataSetInfo.getSchemaDefinitionByFieldName(fieldToUse);

              ctx.addUnnests(
                  this.unnestBuilder.fromRelationshipPath(
                      this.entityTable.getPathToTable(tableToUse), SqlUtil.JoinType.LEFT, true));

              ctx.addPartitions(
                  this.partitionBuilder.fromRelationshipPath(
                      this.entityTable.getPathToTable(tableToUse)));

              if (definition.getMode().equals(Field.Mode.REPEATED.toString())) {
                return tableToUse.getTableAlias(this.dataSetInfo);
              } else {
                return String.format(
                    "%1$s.%2$s AS %3$s",
                    tableToUse.getTableAlias(this.dataSetInfo),
                    definition.getName(),
                    definition.getAlias());
              }
            });
  }

  protected List<CountByField> getCountByFields() {
    TableInfo countFieldsTable =
        filesQuery ? this.dataSetInfo.getTableInfo(DataSetInfo.FILE_PREFIX) : this.entityTable;

    List<CountByField> countByFieldList = new ArrayList<>();

    Queue<SchemaDefinition> schemaDefinitionQueue =
        new LinkedList<>(List.of(countFieldsTable.getSchemaDefinitions()));
    while (schemaDefinitionQueue.size() > 0) {
      SchemaDefinition definition = schemaDefinitionQueue.remove();

      if (definition.isExcludeFromSelect()) {
        continue;
      }

      if (definition.getType().equals(LegacySQLTypeName.RECORD.toString())) {
        schemaDefinitionQueue.addAll(List.of(definition.getFields()));
      } else if (Objects.nonNull(definition.getCountByFields())
          && definition.getCountByFields().length > 0) {
        countByFieldList.addAll(List.of(definition.getCountByFields()));
      }
    }

    return countByFieldList;
  }
}
