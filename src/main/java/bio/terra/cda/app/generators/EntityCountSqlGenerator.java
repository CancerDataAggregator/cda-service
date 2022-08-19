package bio.terra.cda.app.generators;

import bio.terra.cda.app.models.EntitySchema;
import bio.terra.cda.app.models.TableRelationship;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.QueryUtil;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EntityCountSqlGenerator extends SqlGenerator {
  private List<String> countFields;

  public EntityCountSqlGenerator(
      String qualifiedTable, Query rootQuery, String version, boolean filesQuery)
      throws IOException {
    super(qualifiedTable, rootQuery, version, filesQuery);
  }

  @Override
  protected void initializeEntityFields() {
    CountQueryGenerator queryGenerator = this.getClass().getAnnotation(CountQueryGenerator.class);
    this.modularEntity = queryGenerator != null;

      this.entityTable = queryGenerator != null
              ? this.dataSetInfo.getTableInfo(queryGenerator.entity())
              : this.dataSetInfo.getTableInfo(version);

    this.filteredFields =
        queryGenerator != null ? Arrays.asList(queryGenerator.excludedFields()) : List.of();

    this.countFields =
        queryGenerator != null ? Arrays.asList(queryGenerator.fieldsToCount()) : List.of();

    if (filesQuery) {
      this.countFields =
          List.of(TableSchema.SYSTEM_IDENTIFIER, "data_category", "file_format", "data_type");
    }
  }

  @Override
  protected String sql(String tableOrSubClause, Query query, boolean subQuery)
      throws UncheckedExecutionException, IllegalArgumentException {
    String viewSql = super.sql(tableOrSubClause, QueryUtil.deSelectifyQuery(query), subQuery);
    String tableAlias = "flattened_result";
    return subQuery
        ? viewSql
        : String.format(
            "with %s as (%s) select %s", tableAlias, viewSql, getCountSelects(tableAlias));
  }

  protected String getCountSelects(String tableAlias) {
    String formatString =
        "(select ARRAY(select as STRUCT %1$s, count(distinct %2$s) as count "
            + "from %3$s "
            + "group by %1$s)) as %4$s";

    return Stream.concat(Stream.of(this.entityTable.getPartitionKeyAlias()), countFields.stream())
        .distinct()
        .map(
            field -> {
              String name = field.equals(this.entityTable.getPartitionKeyAlias())
                      ? "total"
                      : field.toLowerCase();

              return List.of(this.entityTable.getPartitionKeyAlias(), TableSchema.FILES_COLUMN, TableSchema.FILE_PREFIX)
                      .contains(field)
                  ? String.format(
                      "(SELECT COUNT(DISTINCT %s) from %s) as %s", field, tableAlias, name)
                  : String.format(formatString, field, this.entityTable.getPartitionKeyAlias(), tableAlias, name);
            })
        .collect(Collectors.joining(", "));
  }

  @Override
  protected Stream<String> getSelectsFromEntity(
      QueryContext ctx, String prefix, boolean skipExcludes) {

    return Arrays.stream(ctx.getFilesQuery()
                    ? this.dataSetInfo.getTableInfo(TableSchema.FILE_PREFIX).getSchemaDefinitions()
                    : this.entityTable.getSchemaDefinitions())
            .filter(
                definition ->
                    !(ctx.getFilesQuery()
                            && List.of("ResearchSubject", "Subject", "Specimen")
                                .contains(definition.getName()))
                        && (skipExcludes || !filteredFields.contains(definition.getName())))
            .flatMap(
                definition -> {
                  if (definition.getMode().equals(Field.Mode.REPEATED.toString())) {
                      ctx.addUnnests(this.unnestBuilder.fromRelationshipPath(
                              this.entityTable.getPathToTable(
                                      this.dataSetInfo.getTableInfoFromField(definition.getName())),
                              SqlUtil.JoinType.LEFT, true));
//                    ctx.addUnnests(
//                        this.unnestBuilder.fromParts(
//                            ctx.getFilesQuery() ? fileTable : table,
//                            parts,
//                            !parts[parts.length - 1].equals(TableSchema.FILES_COLUMN),
//                            SqlUtil.JoinType.INNER));

                      ctx.addPartitions(this.partitionBuilder.fromRelationshipPath(this.entityTable.getPathToTable(
                              this.dataSetInfo.getTableInfoFromField(definition.getName()))));

                    return !definition.getType().equals(LegacySQLTypeName.RECORD.toString())
                        ? Stream.of(String.format("%s", definition.getAlias()))
                        : Arrays.stream(definition.getFields())
                            .map(
                                fieldDefinition ->
                                        String.format(
                                                "%1$s.%2$s AS %3$s",
                                                definition.getAlias(),
                                                fieldDefinition.getName(),
                                                fieldDefinition.getAlias()));
                  } else {
                    return Stream.of(
                        String.format("%1$s.%2$s AS %3$s", prefix, definition.getName(), definition.getAlias()));
                  }
                });
  }
}
