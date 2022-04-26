package bio.terra.cda.app.generators;

import bio.terra.cda.app.operators.BasicOperator;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqlGenerator {
  final String qualifiedTable;
  final Query rootQuery;
  final String table;
  final Map<String, TableSchema.SchemaDefinition> tableSchemaMap;
  final List<TableSchema.SchemaDefinition> tableSchema;

  public SqlGenerator(String qualifiedTable, Query rootQuery, String version) throws IOException {
    this.qualifiedTable = qualifiedTable;
    this.rootQuery = rootQuery;
    int dotPos = qualifiedTable.lastIndexOf('.');
    this.table = dotPos == -1 ? qualifiedTable : qualifiedTable.substring(dotPos + 1);
    this.tableSchema = TableSchema.getSchema(version);
    this.tableSchemaMap = TableSchema.buildSchemaMap(this.tableSchema);
  }

  public String generate() throws IllegalArgumentException {
    return sql(qualifiedTable, rootQuery);
  }

  protected String sql(String tableOrSubClause, Query query) throws IllegalArgumentException {
    if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
      // A SUBQUERY is built differently from other queries. The FROM clause is the
      // SQL version of
      // the right subtree, instead of using table. The left subtree is now the top
      // level query.
      return sql(String.format("(%s)", sql(tableOrSubClause, query.getR())), query.getL());
    }

    var fromClause =
        Stream.concat(
                Stream.of(tableOrSubClause + " AS " + table),
                        query.getNodeType() == Query.NodeTypeEnum.SELECT
                                ? ((BasicOperator)query).getUnnestColumns(table, tableSchemaMap).distinct()
                                : getAllUnnestColumns().distinct())
            .collect(Collectors.joining(" "));

    String condition = ((BasicOperator) query).queryString(table, tableSchemaMap);

    return String.format("%s FROM %s WHERE %s", getSelect(query), fromClause, condition);
  }

  protected Stream<String> getAllUnnestColumns() {
    AtomicReference<Stream<String>> unnests = new AtomicReference<>(Stream.of());
    tableSchemaMap.keySet().forEach(key -> {
      var definition = tableSchemaMap.get(key);
      if (definition.getType().equals("RECORD") || definition.getMode().equals("REPEATED")) {
        unnests.set(Stream.concat(
                unnests.get(),
                SqlUtil.getUnnestsFromParts(table, key.split("\\."), true)
        ));
      }
    });

    return unnests.get();
  }

  protected String getSelect(Query query) {
    List<String> selects = null;
    if (query.getNodeType() == Query.NodeTypeEnum.SELECT) {
      selects = queryToSelect(query).collect(Collectors.toList());
    } else {
      selects = new LinkedList<String>();
      getSelectsFromSchema("", tableSchema, selects);
    }

    return String.format("SELECT %s", String.join(", ", selects));
  }

  protected void getSelectsFromSchema(String prefix,
                                      List<TableSchema.SchemaDefinition> definitions,
                                      List<String> selects) {
    definitions.forEach(
            definition -> {
              var prefixName =
                      prefix.isEmpty() ? definition.getName() : String.format("%s.%s", prefix, definition.getName());

              if (definition.getType().equals("RECORD")) {
                getSelectsFromSchema(prefixName, List.of(definition.getFields()), selects);
              } else {
                selects.add(fieldToSelect(prefixName, definition.getMode().equals("REPEATED")));
              }
            });
  }

  protected Stream<String> queryToSelect(Query query) {
    return Arrays.stream(query.getL().getValue().split(","))
        .map(field -> fieldToSelect(field, false));
  }

  protected String fieldToSelect(String select, Boolean repeated) {
    var parts =
            Arrays.stream(select.split("\\.")).map(String::trim).toArray(String[]::new);
    return String.format(
            "%s%s AS %s",
            repeated ? "" : parts.length == 1
              ? String.format("%s.", table)
              : String.format("%s.", SqlUtil.getAlias(parts.length - 2, parts)),
            repeated ? SqlUtil.getAlias(parts.length - 1, parts) : parts[parts.length - 1],
            String.join("_", parts));
  }
}
