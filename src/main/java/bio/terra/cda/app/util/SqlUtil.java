package bio.terra.cda.app.util;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SqlUtil {
  private SqlUtil() {}

  public enum JoinType {
    LEFT("LEFT JOIN"),
    RIGHT("RIGHT JOIN"),
    CROSS("CROSS JOIN"),
    INNER("INNER JOIN");

    public final String value;

    JoinType(String value) {
      this.value = value;
    }
  }

  public static Stream<String> getUnnestsFromParts(
      QueryContext ctx, String table, String[] parts, boolean includeLast) {
    return getUnnestsFromParts(ctx, table, parts, includeLast, JoinType.LEFT);
  }

  public static Stream<String> getUnnestsFromParts(
      QueryContext ctx, String table, String[] parts, boolean includeLast, JoinType JoinType) {
    return IntStream.range(0, parts.length - (includeLast ? 0 : 1))
        .mapToObj(
            i -> {
              String alias = getAlias(i, parts);
              ctx.addAlias(alias, Arrays.stream(parts, 0, i + 1).collect(Collectors.joining(".")));
              return i == 0
                  ? SqlTemplate.unnest(JoinType.value.toUpperCase(), table, parts[i], alias)
                  : SqlTemplate.unnest(JoinType.value.toUpperCase(), getAlias(i - 1, parts), parts[i], alias);
            });
  }

  public static Stream<String> getUnnestsFromPartsWithEntityPath(
      QueryContext ctx, String table, String[] parts, boolean includeLast, String entityPath) {
    return IntStream.range(0, parts.length - (includeLast ? 0 : 1))
        .mapToObj(
            i -> {
              String alias = getAlias(i, parts);
              String partsSub = Arrays.stream(parts, 0, i + 1).collect(Collectors.joining("."));

              ctx.addAlias(alias, partsSub);

              JoinType joinType = entityPath.startsWith(partsSub) ? JoinType.INNER : JoinType.LEFT;

              return i == 0
                  ? SqlTemplate.unnest(joinType.value.toUpperCase(), table, parts[i], alias)
                  : SqlTemplate.unnest(joinType.value.toUpperCase(), getAlias(i - 1, parts), parts[i], alias);
            });
  }

  public static Stream<String> getIdSelectsFromPath(
      QueryContext ctx, String path, Boolean includeLast) {
    String[] parts = SqlUtil.getParts(path);
    return IntStream.range(0, parts.length - (includeLast ? 0 : 1))
        .mapToObj(
            i -> {
              String tmp = getAlias(i, parts).substring(1).toLowerCase();
              String alias = String.format("%s_id", tmp);
              String value = String.format("%s.id", getAlias(i, parts));

              ctx.addAlias(alias, Arrays.stream(parts, 0, i + 1).collect(Collectors.joining(".")));
              return String.format("%s AS %s", value, alias);
            });
  }

  public static Stream<String> getIdSelectsFromPathWithEmpties(
          QueryContext ctx, String path, Boolean includeLast, String[] realPath) {
      String[] parts = path.split("\\.");
      return IntStream.range(0, parts.length - (includeLast ? 0 : 1))
              .mapToObj(
                      i -> {
                          String tmp = getAlias(i, parts).substring(1).toLowerCase();
                          String alias = String.format("%s_id", tmp);
                          String value = realPath.length < parts.length
                                ? "''"
                                : String.format("%s.id", getAlias(i, parts));

                          ctx.addAlias(alias, Arrays.stream(parts, 0, i + 1).collect(Collectors.joining(".")));
                          return String.format("%s AS %s", value, alias);
                      });
  }

  public static String getAlias(Integer index, String[] parts) {
    return "_" + Arrays.stream(parts, 0, index + 1).collect(Collectors.joining("_"));
  }

  public static String getAntiAlias(String alias) {
    String antiAlias = alias;

    if (antiAlias.startsWith("_")) {
      antiAlias = antiAlias.substring(1);
    }

    antiAlias = antiAlias.replace("_", ".");

    return antiAlias;
  }

  public static String[] getParts(String path) {
      if (Objects.isNull(path)) {
          return new String[0];
      }
      return path.trim().split("\\.");
  }
}
