package bio.terra.cda.app.util;

import bio.terra.cda.app.models.ColumnDefinition;
import bio.terra.cda.app.models.ForeignKey;
import bio.terra.cda.app.models.Join;
import org.apache.logging.log4j.util.Strings;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlTemplate {
  private SqlTemplate() {}

  public static String join(Join join) {
    ForeignKey key = join.getKey();
    return String.format(" %1$s %4$s AS %4$s ON %2$s.%3$s = %4$s.%5$s",
        join.getJoinType().value, key.getFromTableName(), key.getFromField(), key.getDestinationTableName(), key.getFields()[0]);
  }

  public static String regularQuery(
      String selectFields, String from, String where, String orderBys) {
    return String.format(
        "SELECT %1$s FROM %2$s WHERE %3$s%4$s",
        selectFields,
        from,
        where,
        !Objects.equals(orderBys, "") ? String.format(" ORDER BY %s", orderBys) : "");
  }

  public static String resultsQuery(
      String selectFields, String from, String where, List<ColumnDefinition> groupBys, String orderBys) {
    return String.format(
        "SELECT %1$s FROM %2$s WHERE %3$s%4$s%5$s",
        selectFields,
        from,
        where,
        groupBys.size() > 0 ? String.format(
            " GROUP BY %s",
            String.join(",", groupBys.stream().distinct().map(col -> String.format("%s.%s", col.getTableName(), col.getName())).collect(Collectors.toList())))
            : "",
        !Objects.equals(orderBys, "") ? String.format(" ORDER BY %s", orderBys) : "");
  }

  public static String countWrapper(String sqlQuery) {
    return String.format(
        "SELECT count(*) from (%1$s) as quantify",
        sqlQuery
    );
  }
  public static String addPagingFields(String sqlQuery, Integer offset, Integer limit) {
    String limitSql = limit != null && limit > 0 ?
        String.format("LIMIT %d", limit)
        : "";
    String offsetSql = offset != null && offset > 1 ?
        String.format("OFFSET %d", offset)
        : "";

    return String.format(
        "%s %s %s",
        sqlQuery,
        offsetSql,
        limitSql
    );
  }

  public static String jsonWrapper(String sqlQuery) {
    return String.format(
        "SELECT row_to_json(json) FROM (%1$s) AS json",
        sqlQuery
    );
  }
}
