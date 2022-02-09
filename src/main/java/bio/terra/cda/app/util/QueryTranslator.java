package bio.terra.cda.app.util;

import bio.terra.cda.generated.model.Query;
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

  public static String sqlCount(String table, Query query) {
    return new SqlGenerator(table, query, Boolean.TRUE).generate();
  }

  // A convenience class to avoid having to pass 'table' around to all the
  // methods.
  private static class SqlGenerator {
    final String qualifiedTable;
    final Query rootQuery;
    final String table;
    Boolean Count;

    private SqlGenerator(String qualifiedTable, Query rootQuery) {
      this.qualifiedTable = qualifiedTable;
      this.rootQuery = rootQuery;
      int dotPos = qualifiedTable.lastIndexOf('.');
      this.table = dotPos == -1 ? qualifiedTable : qualifiedTable.substring(dotPos + 1);
    }

    private SqlGenerator(String qualifiedTable, Query rootQuery, Boolean Count) {
      this.qualifiedTable = qualifiedTable;
      this.rootQuery = rootQuery;
      int dotPos = qualifiedTable.lastIndexOf('.');
      this.table = dotPos == -1 ? qualifiedTable : qualifiedTable.substring(dotPos + 1);
      this.Count = Boolean.TRUE;
    }

    private String generate() {
      return sql(qualifiedTable, rootQuery);
    }

    private String sql(String tableOrSubClause, Query query) {
      if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
        // A SUBQUERY is built differently from other queries. The FROM clause is the
        // SQL version of
        // the right subtree, instead of using table. The left subtree is now the top
        // level query.
        return sql(String.format("(%s)", sql(tableOrSubClause, query.getR())), query.getL());
      }
      var fromClause = Stream.concat(
          Stream.of(tableOrSubClause + " AS " + table), getUnnestColumns(query).distinct())
          .collect(Collectors.joining(", "));

      String condition = null;
      try {
        condition = queryString(query);
      } catch (Exception e) {
        e.printStackTrace();
      }
      if (Count == Boolean.FALSE || Count == null) {
        return String.format("SELECT %s.* FROM %s WHERE %s", table, fromClause, condition);
      } else {
        return String.format(
            "SELECT\n"
                + "  top_level_file.system,\n"
                + "  CASE\n"
                + "    WHEN subject_count IS NULL THEN 0\n"
                + "    ELSE subject_count.count_file\n"
                + "  END AS subject_count,\n"
                + "  CASE\n"
                + "    WHEN top_level_file.count_file IS NULL THEN 0\n"
                + "    ELSE top_level_file.count_file\n"
                + "  END AS subject_file_count,\n"
                + "  CASE\n"
                + "    WHEN researchsubject_count.count_researchsubject IS NULL THEN 0\n"
                + "    ELSE researchsubject_count.count_researchsubject\n"
                + "  END AS researchsubject_count,\n"
                + "  CASE\n"
                + "    WHEN researchsubject_file_count.count_researchsubject IS NULL THEN 0\n"
                + "    ELSE researchsubject_file_count.count_researchsubject\n"
                + "  END AS researchsubject_file_count,\n"
                + "  CASE\n"
                + "    WHEN specimen_count.count_specimen IS NULL THEN 0\n"
                + "    ELSE specimen_count.count_specimen\n"
                + "  END AS specimen_count,\n"
                + "  CASE\n"
                + "    WHEN specimen_file_count.count_file IS NULL THEN 0\n"
                + "    ELSE specimen_file_count.count_file\n"
                + "  END AS specimen_file_count\n"
                + "FROM\n"
                + "  (\n"
                + "    SELECT\n"
                + "      _file_identifier.system,\n"
                + "      COUNT(_file_identifier.value) AS count_file\n"
                + "    FROM\n"
                + "      %2$s AS all_v2_1,\n"
                + "      UNNEST(all_v2_1.File) AS _File,\n"
                + "      UNNEST(_File.identifier) AS _file_identifier\n"
                + "    WHERE\n"
                + "      %1$s\n"
                + "    GROUP BY\n"
                + "      _file_identifier.system\n"
                + "  ) AS top_level_file\n"
                + "  LEFT OUTER JOIN (\n"
                + "    SELECT\n"
                + "      _identifier.system,\n"
                + "      COUNT(_identifier.value) AS count_researchsubject\n"
                + "    FROM\n"
                + "      %2$s AS all_v2_1,\n"
                + "      UNNEST(ResearchSubject) AS _ResearchSubject,\n"
                + "      UNNEST(_ResearchSubject.identifier) AS _identifier\n"
                + "    WHERE\n"
                + "      %1$s\n"
                + "    GROUP BY\n"
                + "      _identifier.system\n"
                + "  ) AS researchsubject_count ON researchsubject_count.system = top_level_file.system\n"
                + "  LEFT OUTER JOIN (\n"
                + "    SELECT\n"
                + "      _file_identifier.system,\n"
                + "      COUNT(_file_identifier.value) AS count_researchsubject\n"
                + "    FROM\n"
                + "      %2$s AS all_v2_1,\n"
                + "      UNNEST(ResearchSubject) AS _ResearchSubject,\n"
                + "      UNNEST(_ResearchSubject.File) AS _researchsubject_file,\n"
                + "      UNNEST(_researchsubject_file.identifier) AS _file_identifier\n"
                + "    WHERE\n"
                + "      %1$s\n"
                + "    GROUP BY\n"
                + "      _file_identifier.system\n"
                + "  ) AS researchsubject_file_count ON researchsubject_file_count.system = top_level_file.system\n"
                + "  LEFT OUTER JOIN (\n"
                + "    SELECT\n"
                + "      _identifier.system,\n"
                + "      COUNT(_identifier.value) AS count_specimen\n"
                + "    FROM\n"
                + "      %2$s AS all_v2_1,\n"
                + "      UNNEST(ResearchSubject) AS _ResearchSubject,\n"
                + "      UNNEST(_ResearchSubject.Specimen) AS _Specimen,\n"
                + "      UNNEST(_Specimen.identifier) AS _identifier\n"
                + "    WHERE\n"
                + "      %1$s\n"
                + "    GROUP BY\n"
                + "      _identifier.system\n"
                + "  ) AS specimen_count ON specimen_count.system = top_level_file.system\n"
                + "  LEFT OUTER JOIN (\n"
                + "    SELECT\n"
                + "      _file_identifier.system,\n"
                + "      COUNT(_file_identifier.value) AS count_file\n"
                + "    FROM\n"
                + "      %2$s AS all_v2_1,\n"
                + "      UNNEST(ResearchSubject) AS _ResearchSubject,\n"
                + "      UNNEST(_ResearchSubject.Specimen) AS _Specimen,\n"
                + "      UNNEST(_Specimen.File) AS _File,\n"
                + "      UNNEST(_File.identifier) AS _file_identifier\n"
                + "    WHERE\n"
                + "      %1$s\n"
                + "    GROUP BY\n"
                + "      _file_identifier.system\n"
                + "  ) AS specimen_file_count ON specimen_file_count.system = top_level_file.system\n"
                + "  LEFT OUTER JOIN (\n"
                + "    SELECT\n"
                + "      _identifier.system,\n"
                + "      COUNT(_identifier.value) AS count_file\n"
                + "    FROM\n"
                + "      %2$s AS all_v2_1,\n"
                + "      UNNEST(identifier) AS _identifier\n"
                + "    WHERE\n"
                + "      %1$s\n"
                + "    GROUP BY\n"
                + "      _identifier.system\n"
                + "  ) AS subject_count ON subject_count.system = top_level_file.system",
            condition, qualifiedTable);
      }
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
                  i -> i == 0
                      ? String.format("UNNEST(%1$s) AS _%1$s", parts[i])
                      : String.format("UNNEST(_%1$s.%2$s) AS _%2$s", parts[i - 1], parts[i]));
        case NOT:
          return getUnnestColumns(query.getL());
        default:
          return Stream.concat(getUnnestColumns(query.getL()), getUnnestColumns(query.getR()));
      }
    }

    private String queryString(Query query) throws IllegalArgumentException {
      switch (query.getNodeType()) {
        case QUOTED:
          String value = query.getValue();
          if (value.contains("days_to_birth") || value.contains("age_at_death")) {
            return String.format("'%s'", value);
          }
          return String.format("UPPER('%s')", value);
        case UNQUOTED:
          return String.format("%s", query.getValue());
        case COLUMN:
          var parts = query.getValue().split("\\.");
          if (parts.length > 1) {
            return String.format("_%s.%s", parts[parts.length - 2], parts[parts.length - 1]);
          }
          // Top level fields must be scoped by the table name, otherwise they could
          // conflict with
          // unnested fields.
          String value_col = query.getValue();
          if (value_col.contains("days_to_birth") || value_col.contains("age_at_death")) {
            return String.format("%s.%s", table, value_col);
          }
          return String.format("UPPER(%s.%s)", table, query.getValue());
        case NOT:
          return String.format("(%s %s)", query.getNodeType(), queryString(query.getL()));
        case IN:
          String right = queryString(query.getR());
          if (right.contains("[") || right.contains("(")) {
            right = right.substring(1, right.length() - 1).replace("\"", "'");
          } else {
            throw new IllegalArgumentException("To use IN you need to add [ or (");
          }

          String left = queryString(query.getL());
          return String.format("(%s IN (%s))", left, right);
        default:
          return String.format(
              "(%s %s %s)",
              queryString(query.getL()), query.getNodeType(), queryString(query.getR()));
      }
    }
  }
}
