package bio.terra.cda.app.util;

import java.util.stream.Collectors;

public class SqlTemplate {
    public static String unnest(String joinType, String alias, String value, String resultingAlias) {
        return String.format("%1$s UNNEST(%2$s.%3$s) AS %4$s", joinType, alias, value, resultingAlias);
    }

    public static String resultsWrapper(String resultsQuery) {
        return "SELECT results.* EXCEPT(rn) FROM (%1$s) as results WHERE rn = 1";
    }

    public static String resultsQuery(String partitionByFields, String selectFields, String from, String where) {
        return String.format(
                "SELECT ROW_NUMBER() OVER (PARTITION BY %1$s) as rn, %2$s FROM %3$s WHERE %4$s",
                partitionByFields,
                selectFields,
                from,
                where);
    }
}
