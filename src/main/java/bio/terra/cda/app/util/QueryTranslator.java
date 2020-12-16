package bio.terra.cda.app.util;

import bio.terra.cda.generated.model.Query;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryTranslator {
  public final String table;
  public final Query query;

  public QueryTranslator(String table, Query query) {
    this.table = table;
    this.query = query;
  }

  public String sql() {
    var fromClause =
        Stream.concat(
                Stream.of(this.table),
                getUnnestColumns(this.query)
                    .filter(Objects::nonNull)
                    .distinct()
                    .map(s -> String.format("UNNEST(%1$s) AS _%1$s", s)))
            .collect(Collectors.joining(", "));

    var condition = queryString(query);
    return String.format("SELECT * FROM %s WHERE %s", fromClause, condition);
  }

  private Stream<String> getUnnestColumns(Query query) {
    if (query.getNodeType() == Query.NodeTypeEnum.VALUE) {
      return Stream.empty();
    }

    if (query.getNodeType() == Query.NodeTypeEnum.COLUMN) {
      return Stream.of(getColumnParent(query.getValue()));
    }

    return Stream.concat(getUnnestColumns(query.getL()), getUnnestColumns(query.getR()));
  }

  public static String getColumnParent(String column) {
    var parts = column.split("\\.");
    if (parts.length > 1) {
      return parts[0];
    }
    return null;
  }

  public static boolean isNumeric(String str) {
    try {
      Double.parseDouble(str);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
    // return str.matches("-?\\d+(\\.\\d+)?"); // match a number with optional '-' and decimal.
    // return str.chars().allMatch( Character::isDigit );
  }

  private String queryString(Query query) {
    if (query.getNodeType() == Query.NodeTypeEnum.VALUE) {
      String val = query.getValue();
      if (isNumeric(val)) {
        return String.format("%s", val);
      } else {
        return String.format("'%s'", val);
      }
    }

    if (query.getNodeType() == Query.NodeTypeEnum.COLUMN) {
      return String.format("_%s", query.getValue());
    }

    return String.format(
        "(%s %s %s)", queryString(query.getL()), query.getNodeType(), queryString(query.getR()));
  }
}
