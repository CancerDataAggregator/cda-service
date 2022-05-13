package bio.terra.cda.app.util;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SqlUtil {
  private SqlUtil() {}

  public static enum JoinType {
    LEFT("LEFT JOIN"),
    RIGHT("RIGHT JOIN"),
    CROSS("CROSS JOIN"),
    INNER("INNER JOIN");

    public final String value;

    private JoinType(String value) {
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
                ctx.addAlias(alias, Arrays.stream(parts, 0, i + 1)
                                          .collect(Collectors.joining(".")));
                return
                    i == 0
                      ? String.format(
                          "%1$s UNNEST(%2$s.%3$s) AS %4$s", JoinType.value.toUpperCase(), table, parts[i], alias)
                      : String.format(
                          "%1$s UNNEST(%2$s.%3$s) AS %4$s",
                          JoinType.value.toUpperCase(), getAlias(i - 1, parts), parts[i], alias);
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
}
