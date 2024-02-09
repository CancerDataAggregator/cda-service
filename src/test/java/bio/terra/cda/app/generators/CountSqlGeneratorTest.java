package bio.terra.cda.app.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;

import bio.terra.cda.app.operators.QueryModule;
import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CountSqlGeneratorTest {
  static final Path TEST_FILES = Paths.get("src/test/resources/query");

  public static final String TABLE = "subjects";

  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new QueryModule());

  public static Stream<Arguments> queryData() {
    return Stream.of(
        Arguments.of(
            "query-lung.json",
            TABLE,
            TABLE,
            "WITH flattened_results as (SELECT diagnosis.id AS diagnosis_id, file.id AS file_id, researchsubject.id AS researchsubject_id, specimen.id AS specimen_id, subject.id AS subject_id, treatment.id AS treatment_id FROM subject AS subject  LEFT JOIN subject_researchsubject AS subject_researchsubject ON subject.integer_id_alias = subject_researchsubject.subject_alias  LEFT JOIN researchsubject AS researchsubject ON subject_researchsubject.researchsubject_alias = researchsubject.integer_id_alias  LEFT JOIN researchsubject_diagnosis AS researchsubject_diagnosis ON researchsubject.id = researchsubject_diagnosis.researchsubject_id  LEFT JOIN diagnosis AS diagnosis ON researchsubject_diagnosis.diagnosis_id = diagnosis.id  LEFT JOIN file_subject AS file_subject ON subject.integer_id_alias = file_subject.subject_alias  LEFT JOIN file AS file ON file_subject.file_alias = file.integer_id_alias  LEFT JOIN researchsubject_specimen AS researchsubject_specimen ON researchsubject.integer_id_alias = researchsubject_specimen.researchsubject_alias  LEFT JOIN specimen AS specimen ON researchsubject_specimen.specimen_alias = specimen.integer_id_alias  LEFT JOIN researchsubject_treatment AS researchsubject_treatment ON researchsubject.id = researchsubject_treatment.researchsubject_id  LEFT JOIN treatment AS treatment ON researchsubject_treatment.treatment_id = treatment.id WHERE (((COALESCE(UPPER(diagnosis.stage), '') = UPPER('IIA')) OR (COALESCE(UPPER(diagnosis.stage), '') = UPPER('IIB'))) AND (COALESCE(UPPER(researchsubject.primary_diagnosis_site), '') = UPPER('Lung'))) GROUP BY diagnosis.id,file.id,researchsubject.id,specimen.id,subject.id,treatment.id) SELECT COUNT(DISTINCT diagnosis_id) AS diagnosis_id_count, COUNT(DISTINCT file_id) AS file_id_count, COUNT(DISTINCT researchsubject_id) AS researchsubject_id_count, COUNT(DISTINCT specimen_id) AS specimen_id_count, COUNT(DISTINCT subject_id) AS subject_id_count, COUNT(DISTINCT treatment_id) AS treatment_id_count FROM flattened_results"));
  }

  @ParameterizedTest
  @MethodSource("queryData")
  void testQuery(String queryFile, String qualifiedTable, String table, String expectedQueryFormat)
      throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve(queryFile));
    String expectedSql = String.format(expectedQueryFormat, qualifiedTable, table);

    Query query = objectMapper.readValue(jsonQuery, Query.class);

    String sql =
        new CountsSqlGenerator(query).getReadableQuerySql();
    assertEquals(expectedSql, sql);
  }
}
