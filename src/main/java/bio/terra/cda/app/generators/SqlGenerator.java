package bio.terra.cda.app.generators;

import bio.terra.cda.app.operators.BasicOperator;
import bio.terra.cda.app.util.QueryContext;
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

  public String generate() {
    return sql(qualifiedTable, rootQuery);
  }

  protected String sql(String tableOrSubClause, Query query) {
    if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
      // A SUBQUERY is built differently from other queries. The FROM clause is the
      // SQL version of
      // the right subtree, instead of using table. The left subtree is now the top
      // level query.
      return sql(String.format("(%s)", sql(tableOrSubClause, query.getR())), query.getL());
    }

    var ctx = new QueryContext(tableSchemaMap, tableOrSubClause, table);
    var queryString = ((BasicOperator) query).buildQuery(ctx);

    var selects = ctx.getSelect();
    var orders = ctx.getOrderBy();
    return String.format(
        "SELECT %s FROM %s WHERE %s%s",
        selects.size() > 0 ? String.join(", ", selects) : String.format("%s.*", table),
        Stream.concat(
                Stream.of(ctx.getTableOrSubClause() + " AS " + ctx.getTable()),
                ctx.getUnnests().stream().distinct())
            .collect(Collectors.joining(", ")),
        queryString,
        orders.size() > 0 ? String.format(" ORDER BY %s", String.join(", ", orders)) : "");
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
