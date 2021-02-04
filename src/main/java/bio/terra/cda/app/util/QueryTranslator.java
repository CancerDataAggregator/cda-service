package bio.terra.cda.app.util;

import bio.terra.cda.generated.model.Query;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryTranslator {

  public static String sql(String table, Query query) {
    var fromClause =
        Stream.concat(
                Stream.of(table),
                getUnnestColumns(query)
                    .distinct()
                    .map(s -> String.format("UNNEST(%1$s) AS _%1$s", s)))
            .collect(Collectors.joining(", "));

    var condition = queryString(query);
    return String.format("SELECT * FROM %s WHERE %s", fromClause, condition);
  }

  private static Stream<String> getUnnestColumns(Query query) {
    switch (query.getNodeType()) {
      case QUOTED:
      case UNQUOTED:
        return Stream.empty();
      case COLUMN:
        if (hasColumnParent(query)) {
          return Stream.of(getColumnParent(query));
        }
        return Stream.empty();
      default:
        return Stream.concat(getUnnestColumns(query.getL()), getUnnestColumns(query.getR()));
    }
  }

  public static boolean hasColumnParent(Query query) {
    return query.getNodeType() == Query.NodeTypeEnum.COLUMN && query.getValue().indexOf('.') != -1;
  }

  public static String getColumnParent(Query query) {
    var parts = query.getValue().split("\\.");
    if (parts.length > 1) {
      return parts[0];
    }
    return null;
  }

  private static String queryString(Query query) {
    switch (query.getNodeType()) {
      case QUOTED:
        return String.format("'%s'", query.getValue());
      case UNQUOTED:
        return String.format("%s", query.getValue());
      case COLUMN:
        var prefix = hasColumnParent(query) ? "_" : "";
        return String.format("%s%s", prefix, query.getValue());
      default:
        return String.format(
            "(%s %s %s)",
            queryString(query.getL()), query.getNodeType(), queryString(query.getR()));
    }
  }
}
