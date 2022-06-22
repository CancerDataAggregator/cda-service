package bio.terra.cda.app.generators;

import bio.terra.cda.app.models.EntitySchema;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.QueryUtil;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.app.models.Unnest;
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

  public EntityCountSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version, false);
  }

  @Override
  protected void initializeEntityFields() {
    CountQueryGenerator queryGenerator = this.getClass().getAnnotation(CountQueryGenerator.class);
    this.modularEntity = queryGenerator != null;

    this.entitySchema =
            queryGenerator != null
                    ? TableSchema.getDefinitionByName(tableSchema, queryGenerator.Entity())
                    : new EntitySchema();

    this.entitySchema.setTable(table);

    this.filteredFields =
        queryGenerator != null ? Arrays.asList(queryGenerator.ExcludedFields()) : List.of();

    this.countFields =
        queryGenerator != null ? Arrays.asList(queryGenerator.FieldsToCount()) : List.of();
  }

  @Override
  protected String sql(String tableOrSubClause, Query query, Boolean subQuery)
      throws UncheckedExecutionException, IllegalArgumentException {
    String viewSql =
        super.sql(tableOrSubClause, QueryUtil.DeSelectifyQuery(query), subQuery);
    String tableAlias = "flattened_result";
    return subQuery
            ? viewSql
            : String.format(
        "with %s as (%s) select %s", tableAlias, viewSql, getCountSelects(tableAlias));
  }

  protected String getCountSelects(String tableAlias) {
    String formatString =
        "(select ARRAY(select as STRUCT %1$s, count(distinct id) as count "
            + "from %2$s "
            + "group by %1$s)) as %3$s";

    return Stream.concat(Stream.of("id"), countFields.stream())
        .distinct()
        .map(
            field -> {
              String[] parts =
                  field.equals(TableSchema.FILES_COLUMN)
                      ? Stream.concat(
                              entitySchema.getPartsStream(),
                              Arrays.stream(SqlUtil.getParts(field)))
                          .collect(Collectors.toList())
                          .toArray(String[]::new)
                      : SqlUtil.getParts(field);
              String name = field.equals("id") ? "total" : parts[parts.length - 1].toLowerCase();

              String countField = field.equals("id") ? "id" : SqlUtil.getAlias(parts.length - 1, parts);

              return List.of("id", TableSchema.FILES_COLUMN, TableSchema.FILE_PREFIX).contains(field)
                  ? String.format(
                      "(SELECT COUNT(DISTINCT %s) from %s) as %s",
                      countField,
                      tableAlias,
                      name)
                  : String.format(formatString, parts[parts.length - 1], tableAlias, name);
            })
        .collect(Collectors.joining(", "));
  }

  @Override
  protected Stream<String> getSelectsFromEntity(
      QueryContext ctx, String prefix, Boolean skipExcludes) {
      List<TableSchema.SchemaDefinition> schema = entitySchema.wasFound()
              ? List.of(entitySchema.getSchemaFields())
              : tableSchema;

      Stream<TableSchema.SchemaDefinition> definitionStream = skipExcludes
              ? schema.stream()
              : schema.stream().filter(definition -> !filteredFields.contains(definition.getName()));

    return definitionStream
            .flatMap(
                definition -> {
                  String[] parts =
                      SqlUtil.getParts(String.format(
                              "%s%s",
                              entitySchema.wasFound() ? String.format("%s.", entitySchema.getPath()) : "",
                              definition.getName()));

                  if (definition.getMode().equals(Field.Mode.REPEATED.toString())) {
                    ctx.addUnnests(
                        this.unnestBuilder.fromParts(
                            table, parts, !parts[parts.length - 1].equals(TableSchema.FILES_COLUMN), SqlUtil.JoinType.INNER));

                    if (parts[parts.length - 1].equals(TableSchema.FILES_COLUMN)) {
                        String filesPrefix = parts.length > 1
                                ? SqlUtil.getAlias(parts.length - 2, parts) : table;

                        ctx.addUnnests(
                                Stream.of(
                                        this.unnestBuilder.of(
                                            SqlUtil.JoinType.LEFT,
                                            String.format(SqlUtil.ALIAS_FIELD_FORMAT,
                                                    filesPrefix,
                                                    parts[parts.length - 1]),
                                            SqlUtil.getAlias(parts.length - 1, parts),
                                            false,
                                            "",
                                            "")));
                    }

                    ctx.addPartitions(this.partitionBuilder.fromParts(parts, tableSchemaMap));

                    return !definition.getType().equals(LegacySQLTypeName.RECORD.toString())
                        ? Stream.of(String.format("%s", SqlUtil.getAlias(parts.length - 1, parts)))
                        : Arrays.stream(definition.getFields())
                            .map(
                                fieldDefinition -> {
                                  String fieldPrefix = SqlUtil.getAlias(parts.length - 1, parts);
                                  return String.format(
                                      "%1$s.%2$s AS %2$s", fieldPrefix, fieldDefinition.getName());
                                });
                  } else {
                    return Stream.of(
                        String.format("%1$s.%2$s AS %2$s", prefix, definition.getName()));
                  }
                });
  }
}
