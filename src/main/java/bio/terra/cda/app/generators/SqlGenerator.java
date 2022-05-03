package bio.terra.cda.app.generators;

import bio.terra.cda.app.operators.BasicOperator;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import com.google.cloud.Tuple;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqlGenerator {
  final String qualifiedTable;
  final Query rootQuery;
  final String table;
  final Map<String, TableSchema.SchemaDefinition> tableSchemaMap;
  final List<TableSchema.SchemaDefinition> tableSchema;
  final Tuple<String, TableSchema.SchemaDefinition> entitySchema;
  final List<String> filteredFields;
  final Boolean modularEntity;

  public SqlGenerator(String qualifiedTable, Query rootQuery, String version) throws IOException {
    this.qualifiedTable = qualifiedTable;
    this.rootQuery = rootQuery;
    int dotPos = qualifiedTable.lastIndexOf('.');
    this.table = dotPos == -1 ? qualifiedTable : qualifiedTable.substring(dotPos + 1);
    this.tableSchema = TableSchema.getSchema(version);
    this.tableSchemaMap = TableSchema.buildSchemaMap(this.tableSchema);

    QueryGenerator queryGenerator = this.getClass().getAnnotation(QueryGenerator.class);
    this.modularEntity = queryGenerator != null;
    this.entitySchema =
        queryGenerator != null
            ? TableSchema.getDefinitionByName(tableSchema, queryGenerator.Entity())
            : null;

    this.filteredFields =
        queryGenerator != null ? Arrays.asList(queryGenerator.ExcludedFields()) : List.of();
  }

  public String generate() throws IllegalArgumentException {
    return sql(qualifiedTable, rootQuery, false);
  }

  protected String sql(String tableOrSubClause, Query query, Boolean subQuery)
      throws IllegalArgumentException {
    var resultsQuery = resultsQuery(query, tableOrSubClause, subQuery);
    var resultsAlias = "results";
    return String.format(
        "SELECT %1$s.* EXCEPT(rn) FROM (%2$s) as %1$s WHERE rn = 1", resultsAlias, resultsQuery);
  }

  protected String resultsQuery(Query query, String tableOrSubClause, Boolean subQuery) {
    if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
      // A SUBQUERY is built differently from other queries. The FROM clause is the
      // SQL version of
      // the right subtree, instead of using table. The left subtree is now the top
      // level query.
      return resultsQuery(
          query.getL(), String.format("(%s)", sql(tableOrSubClause, query.getR(), true)), subQuery);
    }

    String[] parts = entitySchema != null ? entitySchema.x().split("\\.") : new String[0];
    String prefix = entitySchema != null ? SqlUtil.getAlias(parts.length - 1, parts) : table;

    Stream<String> entityUnnests =
        entitySchema != null ? SqlUtil.getUnnestsFromParts(table, parts, true) : Stream.empty();

    var fromClause =
        Stream.concat(
                Stream.concat(
                    Stream.of(baseFromClause(tableOrSubClause)),
                    ((BasicOperator) query).getUnnestColumns(table, tableSchemaMap, true)),
                entityUnnests)
            .distinct()
            .collect(Collectors.joining(" "));

    String condition = ((BasicOperator) query).queryString(table, tableSchemaMap);

    return String.format(
        "SELECT ROW_NUMBER() OVER (PARTITION BY %1$s) as rn, %2$s FROM %3$s WHERE %4$s",
        getPartitionByFields(query, prefix),
        subQuery ? String.format("%s.*", table) : getSelect(query, prefix, !this.modularEntity),
        fromClause,
        condition);
  }

  protected String baseFromClause(String tableOrSubClause) {
    return tableOrSubClause + " AS " + table;
  }

  protected String getPartitionByFields(Query query, String alias) {
    if (query.getNodeType() == Query.NodeTypeEnum.SELECT) {
      return Stream.concat(
              Stream.of(String.format("%s.id", alias)),
              Arrays.stream(query.getL().getValue().split(","))
                  .map(String::trim)
                  .filter(
                      select ->
                          this.tableSchemaMap.get(select).getMode().equals("REPEATED")
                              || select.contains("."))
                  .map(
                      select -> {
                        var parts =
                            Arrays.stream(select.split("\\."))
                                .map(String::trim)
                                .toArray(String[]::new);

                        if (this.tableSchemaMap.get(select).getMode().equals("REPEATED")) {
                          return SqlUtil.getAlias(parts.length - 1, parts);
                        } else if (Arrays.asList(parts).contains("identifier")) {
                          return String.format(
                              "%s.system", SqlUtil.getAlias(parts.length - 2, parts));
                        } else {
                          return String.format("%s.id", SqlUtil.getAlias(parts.length - 2, parts));
                        }
                      }))
          .distinct()
          .collect(Collectors.joining(", "));
    } else {
      return String.format("%s.id", alias);
    }
  }

  protected String getSelect(Query query, String table, Boolean skipExcludes) {
    if (query.getNodeType() == Query.NodeTypeEnum.SELECT) {
      return queryToSelect(query).collect(Collectors.joining(","));
    } else {
      return String.join(", ", getSelectsFromEntity(table, skipExcludes));
    }
  }

  protected Stream<String> queryToSelect(Query query) {
    return Arrays.stream(query.getL().getValue().split(","))
        .map(String::trim)
        .map(
            select -> {
              var mode = tableSchemaMap.get(select).getMode();
              var parts =
                  Arrays.stream(select.split("\\.")).map(String::trim).toArray(String[]::new);
              return String.format(
                  "%1$s%2$s AS %3$s",
                  mode.equals("REPEATED")
                      ? ""
                      : parts.length == 1
                          ? String.format("%s.", table)
                          : SqlUtil.getAlias(parts.length - 2, parts),
                  mode.equals("REPEATED")
                      ? SqlUtil.getAlias(parts.length - 1, parts)
                      : parts[parts.length - 1],
                  this.modularEntity ? parts[parts.length - 1] : String.join("_", parts));
            });
  }

  protected List<String> getSelectsFromEntity(String prefix, Boolean skipExcludes) {
    return (entitySchema != null ? Arrays.asList(entitySchema.y().getFields()) : tableSchema)
        .stream()
            .filter(definition -> skipExcludes || !filteredFields.contains(definition.getName()))
            .map(definition -> String.format("%1$s.%2$s AS %2$s", prefix, definition.getName()))
            .collect(Collectors.toList());
  }
}
