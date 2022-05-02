package bio.terra.cda.app.generators;

import bio.terra.cda.app.operators.BasicOperator;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.generated.model.Query;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CountsSqlGenerator extends SqlGenerator {
  public CountsSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version);
  }

  @Override
  protected String sql(String tableOrSubClause, Query query, Boolean subQuery)
      throws UncheckedExecutionException, IllegalArgumentException {
    if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
      // A SUBQUERY is built differently from other queries. The FROM clause is the
      // SQL version of
      // the right subtree, instead of using table. The left subtree is now the top
      // level query.
      return sql(String.format("(%s)", sql(tableOrSubClause, query.getR(), true)), query.getL(), false);
    }
    Supplier<Stream<String>> fromClause =
        () -> {
          try {
            return Stream.concat(
                Stream.of(tableOrSubClause + " AS " + table),
                ((BasicOperator) query).getUnnestColumns(table, tableSchemaMap, true).distinct());
          } catch (Exception e) {
            throw new UncheckedExecutionException(e);
          }
        };
    String condition = ((BasicOperator) query).queryString(table, tableSchemaMap);

    var whereClause =
        condition != null && condition.length() > 0
            ? String.format("WHERE\n" + "    %s\n", condition)
            : "";

    var countArrays =
        tableSchemaMap.keySet().stream()
            .filter(
                field -> {
                  var splitField = field.split("\\.");
                  if (splitField.length < 2) {
                    return false;
                  }

                  return splitField[1].equals("identifier");
                })
            .map(
                field -> {
                  var splitField = field.split("\\.");
                  return String.join(".", splitField[0], splitField[1]);
                })
            .distinct();

    var selects = new LinkedList<String>();
    var queries = new LinkedList<String>();

    countArrays.forEach(
        field -> {
          var splitField = field.split("\\.");
          var alias = String.format("%s_count", splitField[0].toLowerCase());
          var filesAlias = String.format("%s_files_count", splitField[0].toLowerCase());
          selects.add(getSelectField(alias));
          selects.add(getSelectField(filesAlias));
          // Entity count
          queries.add(getSubQuery(fromClause, whereClause, alias, field, field));
          // File count
          queries.add(getFileSubQuery(fromClause, whereClause, filesAlias, field));
        });

    return String.format("SELECT\n" + " identifiers.system,\n" + "%s", String.join(",\n ", selects))
        + String.format(
            " FROM (\n"
                + "    SELECT DISTINCT _Identifier.system\n"
                + "    FROM %1$s AS %2$s,\n"
                + "    UNNEST(identifier) AS _Identifier\n"
                + ") as identifiers \n"
                + "%3$s",
            tableOrSubClause, table, String.join(" \n ", queries));
  }

  private String getFileSubQuery(
      Supplier<Stream<String>> currentUnnests,
      String whereClause,
      String alias,
      String countByField) {
    String identifierAlias = "_identifier";
    var countBySplit = countByField.split("\\.");

    var from =
        Stream.concat(
                currentUnnests
                    .get()
                    .filter(unnest -> !unnest.contains(String.format("AS %s", identifierAlias))),
                SqlUtil.getUnnestsFromParts(table, countBySplit, true))
            .distinct()
            .collect(Collectors.joining(",\n"));

    var newFrom =
        String.format(
            "%1$s\n" + " INNER JOIN UNNEST(%2$s.identifier) AS %3$s on %3$s.system = %4$s.system\n",
            from, table, identifierAlias, SqlUtil.getAlias(countBySplit.length - 1, countBySplit));

    return String.format(
        "  LEFT OUTER JOIN (\n"
            + "    SELECT\n"
            + "      %1$s.system,\n"
            + "      COUNT(DISTINCT %1$s.value) AS count\n"
            + "    FROM\n"
            + "      %2$s\n"
            + "    %3$s"
            + "    GROUP BY\n"
            + "      %1$s.system\n"
            + "  ) AS %4$s ON %4$s.system = identifiers.system\n",
        identifierAlias, newFrom, whereClause, alias);
  }

  private String getSubQuery(
      Supplier<Stream<String>> currentUnnests,
      String whereClause,
      String alias,
      String groupByField,
      String countByField) {
    var from =
        Stream.concat(
                currentUnnests.get(),
                Stream.concat(
                    SqlUtil.getUnnestsFromParts(table, groupByField.split("\\."), true),
                    SqlUtil.getUnnestsFromParts(table, countByField.split("\\."), true)))
            .distinct()
            .collect(Collectors.joining(",\n"));

    var groupBySplit = groupByField.split("\\.");
    var countBySplit = countByField.split("\\.");

    return String.format(
        "  LEFT OUTER JOIN (\n"
            + "    SELECT\n"
            + "      %1$s.system,\n"
            + "      COUNT(DISTINCT %2$s.value) AS count\n"
            + "    FROM\n"
            + "      %3$s\n"
            + "    %4$s"
            + "    GROUP BY\n"
            + "      %1$s.system\n"
            + "  ) AS %5$s ON %5$s.system = identifiers.system\n",
        SqlUtil.getAlias(groupBySplit.length - 1, groupBySplit),
        SqlUtil.getAlias(countBySplit.length - 1, countBySplit),
        from,
        whereClause,
        alias);
  }

  private String getSelectField(String alias) {
    return String.format(
        "  CASE\n"
            + "    WHEN %1$s.count IS NULL THEN 0\n"
            + "    ELSE %1$s.count\n"
            + "  END AS %1$s\n",
        alias);
  }
}
