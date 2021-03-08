package bio.terra.cda.app.util;

import bio.terra.cda.generated.model.Query;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Class to translate the endpoint Query object to a Big Query query string.
 */
public class QueryTranslator {

  /**
   * Create a SQL query string given a table (or subquery) and a Query object.
   *
   * @param table the table or subquery string to use as the first element of the FROM clause
   * @param query the Query object
   * @return a SQL query string
   */
  public static String sql(String table, Query query) {
    if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
      // A SUBQUERY is built differently from other queries. The FROM clause is the SQL version of
      // the query in R, instead of using table. The L subtree is now the top level query.
      return sql(String.format("(%s)", sql(table, query.getR())), query.getL());
    }
    var fromClause =
        Stream.concat(Stream.of(table), getUnnestColumns(query).distinct())
            .collect(Collectors.joining(", "));

    var condition = queryString(query);
    return String.format("SELECT * FROM %s WHERE %s", fromClause, condition);
  }

  private static Stream<String> getUnnestColumns(Query query) {
    switch (query.getNodeType()) {
      case QUOTED:
      case UNQUOTED:
      case NOT:
        return Stream.empty();
      case COLUMN:
        var parts = query.getValue().split("\\.");
        return IntStream.range(0, parts.length - 1)
            .mapToObj(
                i ->
                    i == 0
                        ? String.format("UNNEST(%1$s) AS _%1$s", parts[i])
                        : String.format("UNNEST(_%1$s.%2$s) AS _%2$s", parts[i - 1], parts[i]));
      default:
        return Stream.concat(getUnnestColumns(query.getL()), getUnnestColumns(query.getR()));
    }
  }

  private static String queryString(Query query) {
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
        return query.getValue();
      case NOT:
        return String.format("(%s %s)", query.getNodeType(), queryString(query.getL()));
      default:
        return String.format(
            "(%s %s %s)",
            queryString(query.getL()), query.getNodeType(), queryString(query.getR()));
    }
  }
}
