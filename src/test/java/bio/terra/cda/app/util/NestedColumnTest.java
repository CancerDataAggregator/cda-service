package bio.terra.cda.app.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NestedColumnTest {
  private static final Logger logger = LoggerFactory.getLogger(NestedColumnTest.class);

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
    StringBuffer unnestClause = new StringBuffer();
    Set<String> unnestClauses = result.getUnnestClauses();
    if (unnestClauses != null) {
      unnestClauses.stream().forEach((s) -> unnestClause.append(s));
    }
    assertEquals(column, result.getColumn());
    assertEquals(clause, unnestClause.toString());
  }

  @Test
  public void testIllegalArgCondition() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> NestedColumn.generate(null));
  }
}
