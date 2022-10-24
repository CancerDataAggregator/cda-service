package bio.terra.cda.app.util;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

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

  public static String getAlias(Integer index, String[] parts) {
    return "_" + Arrays.stream(parts, 0, index + 1).collect(Collectors.joining("_"));
  }

  public static String[] getParts(String path) {
    if (Objects.isNull(path)) {
      return new String[0];
    }
    return path.trim().split("\\.");
  }
}
