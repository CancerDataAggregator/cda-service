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

  public static final String ALIAS_FIELD_FORMAT = "%s.%s";

  public static Stream<String> getIdSelectsFromPath(
      String path, Boolean includeLast) {
    String[] parts = SqlUtil.getParts(path);
    return IntStream.range(0, parts.length - (includeLast ? 0 : 1))
        .mapToObj(
            i -> {
              String tmp = getAlias(i, parts).substring(1).toLowerCase();
              String alias = String.format("%s_id", tmp);
              String value = String.format("%s.id", getAlias(i, parts));

              return String.format("%s AS %s", value, alias);
            });
  }

  public static String getPathFromParts(Integer index, String[] parts) {
      return Arrays.stream(parts, 0, index + 1).collect(Collectors.joining("."));
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
