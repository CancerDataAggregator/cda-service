package bio.terra.cda.app.util;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SqlUtil {
    public static Stream<String> getUnnestsFromParts(String table, String[] parts, Boolean includeLast) {
        return IntStream.range(0, parts.length - (includeLast ? 0 : 1))
                .mapToObj(
                        i -> i == 0
                                ? String.format("UNNEST(%1$s.%2$s) AS %3$s", table, parts[i], getAlias(i, parts))
                                : String.format("UNNEST(%1$s.%2$s) AS %3$s", getAlias(i - 1, parts), parts[i],
                                getAlias(i, parts)));
    }

    public static String getAlias(Integer index, String[] parts) {
        return "_" + Arrays.stream(parts, 0, index + 1).collect(Collectors.joining("_"));
    }
}
