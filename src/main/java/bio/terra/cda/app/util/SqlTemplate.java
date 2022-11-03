package bio.terra.cda.app.util;

import java.util.Objects;

public class SqlTemplate {
  private SqlTemplate() {}

  public static String unnest(
      String joinType, String path, String resultingAlias, String joinPath) {
    return String.format(
        "%1$s UNNEST(%2$s) AS %3$s%4$s",
        joinType,
        path,
        resultingAlias,
        Objects.nonNull(joinPath) && joinPath.length() > 0
            ? String.format(" ON %s", joinPath)
            : "");
  }

  public static String join(String joinType, String path, String alias, String joinPath) {
    return String.format("%1$s %2$s AS %3$s ON %4$s", joinType, path, alias, joinPath);
  }

  public static String resultsWrapper(String resultsQuery) {
    return String.format(
        "SELECT results.* EXCEPT(rn) FROM (%1$s) as results WHERE rn = 1", resultsQuery);
  }

  public static String regularQuery(String selectFields, String from, String where, String orderBys) {
    return String.format("SELECT %1$s FROM %2$s WHERE %3$s%4$s",
            selectFields,
            from,
            where,
            !Objects.equals(orderBys, "") ? String.format(" ORDER BY %s", orderBys) : "");
  }

  public static String resultsQuery(
      String partitionByFields, String selectFields, String from, String where, String orderBys) {
    return String.format(
        "SELECT ROW_NUMBER() OVER (PARTITION BY %1$s) as rn, %2$s FROM %3$s WHERE %4$s%5$s",
        partitionByFields,
        selectFields,
        from,
        where,
        !Objects.equals(orderBys, "") ? String.format(" ORDER BY %s", orderBys) : "");
  }
}
