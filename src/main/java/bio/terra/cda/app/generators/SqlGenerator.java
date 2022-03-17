package bio.terra.cda.app.generators;

import bio.terra.cda.app.operators.BasicOperator;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
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
                ((BasicOperator) query).getUnnestColumns(table, tableSchemaMap).distinct())
            .collect(Collectors.joining(", "));

    String condition = ((BasicOperator) query).queryString(table, tableSchemaMap);

    return String.format("%s FROM %s WHERE %s", getSelect(query, table), fromClause, condition);
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
}
