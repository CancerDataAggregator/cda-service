package bio.terra.cda.app.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class NestedColumnTest {

  private static Stream<Arguments> unnestData() {
    return Stream.of(
        Arguments.of("A.B.C", "_B.C", ", UNNEST(A) AS _A, UNNEST(_A.B) AS _B"),
        Arguments.of("A.B", "_A.B", ", UNNEST(A) AS _A"),
        Arguments.of("A", "A", ""),
        Arguments.of("", "", ""));
  }

  @ParameterizedTest
  @MethodSource("unnestData")
  public void testGeneratedUnnestClause(String qualifiedName, String column, String clause)
      throws Exception {
    NestedColumn result = NestedColumn.generate(qualifiedName);
    assertEquals(column, result.getColumn());
    assertEquals(clause, result.getUnnestClause());
  }

  @Test
  public void testIllegalArgCondition() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> NestedColumn.generate(null));
  }
}
