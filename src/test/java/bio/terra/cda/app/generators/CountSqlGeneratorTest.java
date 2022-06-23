package bio.terra.cda.app.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;

import bio.terra.cda.app.operators.QueryModule;
import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CountSqlGeneratorTest {
  static final Path TEST_FILES = Paths.get("src/test/resources/query");

  public static final String TABLE = "all_Subjects_v3_0_w_RS";
  public static final String QUALIFIED_TABLE = "gdc-bq-sample.dev." + TABLE;

  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new QueryModule());

  public static Stream<Arguments> queryData() {
    return Stream.of(
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            "with flattened_results as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY all_Subjects_v3_0_w_RS.id, _ResearchSubject_Specimen.id, _ResearchSubject_Diagnosis_Treatment.id, _ResearchSubject_Diagnosis.id, _ResearchSubject.id) as rn, _ResearchSubject_Specimen.id AS ResearchSubject_Specimen_id, _ResearchSubject_Diagnosis_Treatment.id AS ResearchSubject_Diagnosis_Treatment_id, _ResearchSubject_Diagnosis.id AS ResearchSubject_Diagnosis_id, _ResearchSubject.id AS ResearchSubject_id, all_Subjects_v3_0_w_RS.id AS id FROM gdc-bq-sample.dev.all_Subjects_v3_0_w_RS AS all_Subjects_v3_0_w_RS LEFT JOIN UNNEST(all_Subjects_v3_0_w_RS.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.Specimen) AS _ResearchSubject_Specimen LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _ResearchSubject_Diagnosis LEFT JOIN UNNEST(_ResearchSubject_Diagnosis.Treatment) AS _ResearchSubject_Diagnosis_Treatment WHERE (((IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER('Stage I')) OR (IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER('Stage II'))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER('Kidney')))) as results WHERE rn = 1) SELECT COUNT(DISTINCT ResearchSubject_Specimen_id) AS specimen_count, COUNT(DISTINCT ResearchSubject_Diagnosis_Treatment_id) AS treatment_count, COUNT(DISTINCT ResearchSubject_Diagnosis_id) AS diagnosis_count, COUNT(DISTINCT ResearchSubject_id) AS researchsubject_count, COUNT(DISTINCT id) AS subject_count FROM flattened_results"));
  }

  @ParameterizedTest
  @MethodSource("queryData")
  void testQuery(String queryFile, String qualifiedTable, String table, String expectedQueryFormat)
      throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve(queryFile));
    String expectedSql = String.format(expectedQueryFormat, qualifiedTable, table);

    Query query = objectMapper.readValue(jsonQuery, Query.class);

    String translatedQuery = new CountsSqlGenerator(qualifiedTable, query, table).generate();

    assertEquals(expectedSql, translatedQuery);
  }
}
