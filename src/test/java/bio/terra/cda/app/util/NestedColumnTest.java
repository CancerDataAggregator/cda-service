package bio.terra.cda.app.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class NestedColumnTest {

  @Test
  public void testGeneratedUnnestClause() throws Exception {
    String sample1 = "A.B.C";
    String sample2 = "A.B";
    String sample3 = "A";

    NestedColumn result1 = new NestedColumn().generate(sample1);
    assertEquals(result1.getColumn(), "_B.C");
    assertEquals(result1.getUnnestClause(), ", UNNEST(A) AS _A, UNNEST(_A.B) AS _B");

    NestedColumn result2 = new NestedColumn().generate(sample2);
    assertEquals(result2.getColumn(), "_A.B");
    assertEquals(result2.getUnnestClause(), ", UNNEST(A) AS _A");

    NestedColumn result3 = new NestedColumn().generate(sample3);
    assertEquals(result3.getColumn(), "A");
    assertEquals(result3.getUnnestClause(), "");
  }

  @Test
  public void testIllegalArgCondition() throws Exception {
    String sample4 = null;
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new NestedColumn().generate(sample4);
        });
  }
}
