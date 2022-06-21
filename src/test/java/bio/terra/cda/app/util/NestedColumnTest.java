package bio.terra.cda.app.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NestedColumnTest {

  private static Stream<Arguments> unnestData() {
    return Stream.of(
        Arguments.of("A.B.C", "_B.C", ", UNNEST(A) AS _A, UNNEST(_A.B) AS _B"),
        Arguments.of("A.B", "_A.B", ", UNNEST(A) AS _A"),
        Arguments.of("A", "A", ""),
        Arguments.of("", "", ""));
  }

  @ParameterizedTest
  @MethodSource("unnestData")
  void testGeneratedUnnestClause(String qualifiedName, String column, String clause)
      throws Exception {
    NestedColumn result = NestedColumn.generate(qualifiedName);
    StringBuffer unnestClause = new StringBuffer();
    Set<String> unnestClauses = result.getUnnestClauses();
    unnestClauses.stream().forEach((s) -> unnestClause.append(s));
    assertEquals(column, result.getColumn());
    assertEquals(clause, unnestClause.toString());
  }

  @Test
  void testIllegalArgCondition() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> NestedColumn.generate(null));
  }
}
