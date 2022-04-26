package bio.terra.cda.app.generators;

import bio.terra.cda.app.operators.BasicOperator;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.generated.model.Query;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.Arrays;
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
  protected String sql(String tableOrSubClause, Query query)
      throws UncheckedExecutionException, IllegalArgumentException {
    if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
      // A SUBQUERY is built differently from other queries. The FROM clause is the
      // SQL version of
      // the right subtree, instead of using table. The left subtree is now the top
      // level query.
      return sql(String.format("(%s)", sql(tableOrSubClause, query.getR())), query.getL());
    }

    String withAlias = "files_filtered";

    String tempTableSql = super.sql(tableOrSubClause, query);
    var withStatement = String.format("with %1$s as (\n"
        + " %2$s\n"
        + ")\n ", withAlias, tempTableSql);

    var countArrays =
        tableSchemaMap.keySet().stream()
            .filter(field -> field.equals("identifier") || field.contains(".identifier."))
            .map(
                field -> {
                  var splitField = field.split("\\.");
                  var indexOfIdentifier = 0;
                  for (String sf: splitField) {
                    indexOfIdentifier++;

                    if (sf.equals("identifier")) {
                      break;
                    }
                  }

                  return Arrays.stream(splitField, 0, indexOfIdentifier).collect(Collectors.joining("."));
                })
            .distinct();

    var selects = new LinkedList<String>();
    var queries = new LinkedList<String>();

    countArrays.forEach(
        field -> {
          var splitField = field.split("\\.");
          var alias = splitField[0].equals("identifier")
            ? table.contains("Files")
              ? "file_count"
              : "subject_count"
            : String.format("%s_count", splitField[splitField.length - 2].toLowerCase());

          selects.add(getSelectField(alias));
          // Entity count
          queries.add(getSubQuery(withAlias, alias, field, field));
        });

    return String.format("%sSELECT\n" + " identifiers.system,\n" + "%s", withStatement, String.join(",\n ", selects))
        + String.format(
            " FROM (\n"
                + "    SELECT DISTINCT _Identifier.system\n"
                + "    FROM %1$s AS %2$s,\n"
                + "    UNNEST(identifier) AS _Identifier\n"
                + ") as identifiers \n"
                + "%3$s",
            tableOrSubClause, table, String.join(" \n ", queries));
  }

  private String getSubQuery(
      String withAlias,
      String alias,
      String groupByField,
      String countByField) {
    var from =
        Stream.concat(
                Stream.of(withAlias),
                Stream.concat(
                    SqlUtil.getUnnestsFromParts(withAlias, groupByField.split("\\."), true),
                    SqlUtil.getUnnestsFromParts(withAlias, countByField.split("\\."), true)))
            .distinct()
            .collect(Collectors.joining(",\n"));

    var groupBySplit = groupByField.split("\\.");
    var countBySplit = countByField.split("\\.");

    return String.format(
        "  LEFT OUTER JOIN (\n"
            + "    SELECT\n"
            + "      %1$s_system as system,\n"
            + "      COUNT(DISTINCT %2$s_value) AS count\n"
            + "    FROM\n"
            + "      %3$s\n"
            + "    GROUP BY\n"
            + "      %1$s_system\n"
            + "  ) AS %4$s ON %4$s.system = identifiers.system\n",
        String.join("_", groupBySplit),
        //SqlUtil.getAlias(groupBySplit.length - 1, groupBySplit),
        String.join("_", countBySplit),
        //SqlUtil.getAlias(countBySplit.length - 1, countBySplit),
        withAlias,
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
