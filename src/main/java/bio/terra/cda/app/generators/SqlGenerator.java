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

  public SqlGenerator(String qualifiedTable, Query rootQuery, String version) throws IOException {
    this.qualifiedTable = qualifiedTable;
    this.rootQuery = rootQuery;
    int dotPos = qualifiedTable.lastIndexOf('.');
    this.table = dotPos == -1 ? qualifiedTable : qualifiedTable.substring(dotPos + 1);
    this.tableSchema = TableSchema.getSchema(version);
    this.tableSchemaMap = TableSchema.buildSchemaMap(this.tableSchema);

    QueryGenerator queryGenerator = this.getClass().getAnnotation(QueryGenerator.class);
    this.entitySchema = queryGenerator != null
            ? TableSchema.getDefinitionByName(tableSchema, queryGenerator.Entity())
            : null;

    this.filteredFields = queryGenerator != null
            ? Arrays.asList(queryGenerator.ExcludedFields())
            : List.of();
  }

  public String generate() throws IllegalArgumentException {
    return sql(qualifiedTable, rootQuery);
  }

  protected String sql(String tableOrSubClause, Query query) throws IllegalArgumentException {
    var resultsQuery = resultsQuery(query, tableOrSubClause);
    var resultsAlias = "results";
    var selects = getSelectsFromEntity(resultsAlias);
    return String.format("SELECT %s FROM (%s) as %s WHERE rn = 1",
            String.join(", ", selects), resultsQuery, resultsAlias);
  }

  protected String resultsQuery(Query query, String tableOrSubClause) {
    if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
      // A SUBQUERY is built differently from other queries. The FROM clause is the
      // SQL version of
      // the right subtree, instead of using table. The left subtree is now the top
      // level query.
      return resultsQuery(query.getL(), String.format("(%s)", sql(tableOrSubClause, query.getR())));
    }

    String[] parts = entitySchema != null
            ? entitySchema.x().split("\\.")
            : new String[0];
    String prefix = entitySchema != null
            ? SqlUtil.getAlias(parts.length - 1, parts)
            : table;

    Stream<String> entityUnnests = entitySchema != null
            ? SqlUtil.getUnnestsFromParts(table, parts, true)
            : Stream.empty();

    var fromClause =
            Stream.concat(
                    Stream.concat(Stream.of(baseFromClause(tableOrSubClause)),
                                  ((BasicOperator) query).getUnnestColumns(table, tableSchemaMap, false)),
                    entityUnnests)
                    .distinct()
                    .collect(Collectors.joining(" "));

    String condition = ((BasicOperator) query).queryString(table, tableSchemaMap);

    return String.format("SELECT ROW_NUMBER() OVER (PARTITION BY %1$s.id) as rn, %1$s.* FROM %2$s WHERE %3$s", prefix, fromClause, condition);
  }

  protected String baseFromClause(String tableOrSubClause) {
    return tableOrSubClause + " AS " + table;
  }

  protected String getSelect(Query query, String table) {
    if (query.getNodeType() == Query.NodeTypeEnum.SELECT) {
      return String.format("SELECT %s", queryToSelect(query).collect(Collectors.joining(",")));
    } else {
      return String.format("SELECT %s.*", table);
    }
  }

  protected Stream<String> queryToSelect(Query query) {
    return Arrays.stream(query.getL().getValue().split(","))
        .map(
            select -> {
              var parts =
                  Arrays.stream(select.split("\\.")).map(String::trim).toArray(String[]::new);
              return String.format(
                  "%s.%s AS %s",
                  parts.length == 1 ? table : SqlUtil.getAlias(parts.length - 2, parts),
                  parts[parts.length - 1],
                  String.join("_", parts));
            });
  }

  protected List<String> getSelectsFromEntity(String prefix) {
    return (entitySchema != null
            ? Arrays.asList(entitySchema.y().getFields())
            : tableSchema).stream()
            .filter(definition -> !filteredFields.contains(definition.getName()))
            .map(definition -> String.format("%1$s.%2$s AS %2$s",
                    prefix, definition.getName()))
            .collect(Collectors.toList());
  }
}
