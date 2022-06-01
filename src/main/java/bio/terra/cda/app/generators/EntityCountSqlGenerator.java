package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.QueryUtil;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class EntityCountSqlGenerator extends SqlGenerator {
  private List<String> countFields;

  public EntityCountSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version);
  }

  @Override
  protected void initializeEntityFields() {
    CountQueryGenerator queryGenerator = this.getClass().getAnnotation(CountQueryGenerator.class);
    this.modularEntity = queryGenerator != null;
    this.entitySchema =
        queryGenerator != null
            ? TableSchema.getDefinitionByName(tableSchema, queryGenerator.Entity())
            : null;

    this.filteredFields =
        queryGenerator != null ? Arrays.asList(queryGenerator.ExcludedFields()) : List.of();

    this.countFields =
        queryGenerator != null ? Arrays.asList(queryGenerator.FieldsToCount()) : List.of();
  }

  protected List<String> getCountFields() {
    return countFields;
  }

  @Override
  protected String sql(String tableOrSubClause, Query query, Boolean subQuery, Boolean filesQuery)
      throws UncheckedExecutionException, IllegalArgumentException {
    String viewSql =
        super.sql(tableOrSubClause, QueryUtil.DeSelectifyQuery(query), subQuery, filesQuery);
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
                  field.equals("Files")
                      ? Stream.concat(
                              entitySchema.getPartsStream(),
                              Arrays.stream(SqlUtil.getParts(field)))
                          .collect(Collectors.toList())
                          .toArray(String[]::new)
                      : SqlUtil.getParts(field);
              String name = field.equals("id") ? "total" : parts[parts.length - 1].toLowerCase();
              TableSchema.SchemaDefinition schemaDefinition = tableSchemaMap.get(field);

              return List.of("id", "Files", "File").contains(field)
                  ? String.format(
                      "(SELECT COUNT(DISTINCT %s) from %s) as %s",
                      field.equals("id") ? "id" : SqlUtil.getAlias(parts.length - 1, parts),
                      tableAlias,
                      name)
                  : String.format(formatString, parts[parts.length - 1], tableAlias, name);
            })
        .collect(Collectors.joining(", "));
  }

  @Override
  protected Stream<String> getSelectsFromEntity(
      QueryContext ctx, String prefix, Boolean skipExcludes) {
    return (entitySchema.wasFound() ? List.of(entitySchema.getSchemaFields()) : tableSchema)
        .stream()
            .filter(definition -> (skipExcludes || !filteredFields.contains(definition.getName())))
            .flatMap(
                definition -> {
                  String[] parts =
                      SqlUtil.getParts(String.format(
                              "%s%s",
                              entitySchema.wasFound() ? String.format("%s.", entitySchema.getPath()) : "",
                              definition.getName()));

                  if (definition.getMode().equals("REPEATED")) {
                    ctx.addUnnests(
                        SqlUtil.getUnnestsFromParts(
                            ctx, table, parts, !parts[parts.length - 1].equals("Files"), SqlUtil.JoinType.INNER));

                    if (parts[parts.length - 1].equals("Files")) {
                        ctx.addUnnests(Stream.of(String.format(
                                "%1$s UNNEST(%2$s.%3$s) AS %4$s",
                                SqlUtil.JoinType.LEFT.value.toUpperCase(),
                                parts.length > 1 ? SqlUtil.getAlias(parts.length - 2, parts) : table,
                                parts[parts.length - 1],
                                SqlUtil.getAlias(parts.length - 1, parts))));
                    }

                    ctx.addPartitions(
                        IntStream.range(0, parts.length)
                            .mapToObj(i -> {
                                if (parts[i].equals("identifier")) {
                                    return String.format("%s.system", SqlUtil.getAlias(i, parts));
                                } else if (!tableSchemaMap.get(
                                        Arrays.stream(parts, 0, i + 1)
                                                .collect(Collectors.joining("."))).getType().equals("RECORD")){
                                    return SqlUtil.getAlias(i, parts);
                                }
                                return String.format("%s.id", SqlUtil.getAlias(i, parts));
                            }));

                    return !definition.getType().equals("RECORD")
                        ? Stream.of(String.format("%s", SqlUtil.getAlias(parts.length - 1, parts)))
                        : Arrays.stream(definition.getFields())
                            .map(
                                fieldDefinition -> {
                                  String fieldPrefix = SqlUtil.getAlias(parts.length - 1, parts);
                                  ctx.addAlias(
                                      fieldDefinition.getName(),
                                      String.format(
                                          "%s.%s",
                                          SqlUtil.getAntiAlias(fieldPrefix),
                                          fieldDefinition.getName()));
                                  return String.format(
                                      "%1$s.%2$s AS %2$s", fieldPrefix, fieldDefinition.getName());
                                });
                  } else {
                    ctx.addAlias(
                        definition.getName(),
                        String.format(
                            "%s%s",
                            prefix.equals(table)
                                ? String.format("%s.", prefix)
                                : String.format("%s.", SqlUtil.getAntiAlias(prefix)),
                            definition.getName()));
                    return Stream.of(
                        String.format("%1$s.%2$s AS %2$s", prefix, definition.getName()));
                  }
                });
  }
}
