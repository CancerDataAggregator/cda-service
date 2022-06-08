package bio.terra.cda.app.util;

public class SqlTemplate {
    public static String unnest(String joinType, String path, String resultingAlias) {
        return String.format("%1$s UNNEST(%2$s) AS %3$s", joinType, path, resultingAlias);
    }

    public static String join(String joinType, String path, String alias, String firstJoinPath, String secondJoinPath) {
        return String.format("%1$s %2$s AS %3$s ON %4$s = %5$s", joinType, path, alias, firstJoinPath, secondJoinPath);
    }

    public static String resultsWrapper(String resultsQuery) {
        return String.format("SELECT results.* EXCEPT(rn) FROM (%1$s) as results WHERE rn = 1", resultsQuery);
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
