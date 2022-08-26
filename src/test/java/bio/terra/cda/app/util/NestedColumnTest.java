package bio.terra.cda.app.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;
import java.util.stream.Stream;

import bio.terra.cda.app.helpers.Schemas;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class NestedColumnTest {

  private static Stream<Arguments> unnestData() {
    return Stream.of(
        Arguments.of("ResearchSubject.Specimen.source_material_type", "_Specimen.source_material_type", ", UNNEST(ResearchSubject) AS _ResearchSubject, UNNEST(_ResearchSubject.Specimen) AS _Specimen"),
        Arguments.of("ResearchSubject.Specimen.specimen_type", "_Specimen.specimen_type", ", UNNEST(ResearchSubject) AS _ResearchSubject, UNNEST(_ResearchSubject.Specimen) AS _Specimen"),
        Arguments.of("sex", "sex", ""));
  }

  @ParameterizedTest
  @MethodSource("unnestData")
  void testGeneratedUnnestClause(String qualifiedName, String column, String clause)
      throws Exception {
    Schemas schemas =
            new Schemas.SchemaBuilder("all_Subjects_v3_0_final", "all_Files_v3_0_final").build();
    NestedColumn result = NestedColumn.generate(qualifiedName,schemas.getSchemaMap());
    StringBuffer unnestClause = new StringBuffer();
    Set<String> unnestClauses = result.getUnnestClauses();
    unnestClauses.stream().forEach((s) -> unnestClause.append(s));
    assertEquals(column, result.getColumn());
    assertEquals(clause, unnestClause.toString());
  }

  @Test
  void testIllegalArgCondition() throws Exception {
    Schemas schemas =
            new Schemas.SchemaBuilder("all_Subjects_v3_0_final", "all_Files_v3_0_final").build();
    assertThrows(IllegalArgumentException.class, () -> NestedColumn.generate(null,schemas.getSchemaMap()));
  }
}
