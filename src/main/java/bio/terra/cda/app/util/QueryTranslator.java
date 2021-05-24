package bio.terra.cda.app.util;

import bio.terra.cda.generated.model.Query;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Class to translate the endpoint Query object to a Big Query query string. */
public class QueryTranslator {

  /**
   * Create a SQL query string given a table (or subquery) and a Query object.
   *
   * @param table the table to use as the first element of the FROM clause
   * @param query the Query object
   * @return a SQL query string
   */
  public static String sql(String table, Query query) {
    return new SqlGenerator(table, query).generate();
  }

  /** Split the TABLE.ColumnName into component parts */
  public static Map<String, String> parseTableName(String qualifiedTable) {
    int dotPos = qualifiedTable.lastIndexOf('.');
    String table = qualifiedTable.substring(0, dotPos);
    String column = qualifiedTable.substring(dotPos + 1);

    return Map.of("columnName", qualifiedTable.substring(dotPos + 1), "tableName", table);
  }

  // A convenience class to avoid having to pass 'table' around to all the methods.
  private static class SqlGenerator {
    final String qualifiedTable;
    final Query rootQuery;
    final String table;

    private SqlGenerator(String qualifiedTable, Query rootQuery) {
      this.qualifiedTable = qualifiedTable;
      this.rootQuery = rootQuery;
      int dotPos = qualifiedTable.lastIndexOf('.');
      this.table = dotPos == -1 ? qualifiedTable : qualifiedTable.substring(dotPos + 1);
    }

    private String generate() {
      return sql(qualifiedTable, rootQuery);
    }

    private String sql(String tableOrSubClause, Query query) {
      if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
        // A SUBQUERY is built differently from other queries. The FROM clause is the SQL version of
        // the right subtree, instead of using table. The left subtree is now the top level query.
        return sql(String.format("(%s)", sql(tableOrSubClause, query.getR())), query.getL());
      }
      var fromClause =
          Stream.concat(
                  Stream.of(tableOrSubClause + " AS " + table), getUnnestColumns(query).distinct())
              .collect(Collectors.joining(", "));

      var condition = queryString(query);
      return String.format("SELECT %s.* FROM %s WHERE %s", table, fromClause, condition);
    }

    private Stream<String> getUnnestColumns(Query query) {
      switch (query.getNodeType()) {
        case QUOTED:
        case UNQUOTED:
          return Stream.empty();
        case COLUMN:
          var parts = query.getValue().split("\\.");
          return IntStream.range(0, parts.length - 1)
              .mapToObj(
                  i ->
                      i == 0
                          ? String.format("UNNEST(%1$s) AS _%1$s", parts[i])
                          : String.format("UNNEST(_%1$s.%2$s) AS _%2$s", parts[i - 1], parts[i]));
        case NOT:
          return getUnnestColumns(query.getL());
        default:
          return Stream.concat(getUnnestColumns(query.getL()), getUnnestColumns(query.getR()));
      }
    }

    private String queryString(Query query) {
      switch (query.getNodeType()) {
        case QUOTED:
          return String.format("'%s'", query.getValue());
        case UNQUOTED:
          return String.format("%s", query.getValue());
        case COLUMN:
          var parts = query.getValue().split("\\.");
          if (parts.length > 1) {
            return String.format("_%s.%s", parts[parts.length - 2], parts[parts.length - 1]);
          }
          // Top level fields must be scoped by the table name, otherwise they could conflict with
          // unnested fields.
          return String.format("%s.%s", table, query.getValue());
        case NOT:
          return String.format("(%s %s)", query.getNodeType(), queryString(query.getL()));
        default:
          return String.format(
              "(%s %s %s)",
              queryString(query.getL()), query.getNodeType(), queryString(query.getR()));
      }
    }
  }
}
