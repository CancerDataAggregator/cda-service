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
            "WITH flattened_results as (SELECT subject.id AS subject_id, subject.species AS species, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.days_to_birth AS days_to_birth, subject.vital_status AS vital_status, subject.days_to_death AS days_to_death, subject.cause_of_death AS cause_of_death FROM subject AS subject, subject_researchsubject AS subject_researchsubject , researchsubject AS researchsubject , researchsubject_diagnosis AS researchsubject_diagnosis , diagnosis AS diagnosis , file_subject AS file_subject , file AS file , researchsubject_specimen AS researchsubject_specimen , specimen AS specimen , researchsubject_treatment AS researchsubject_treatment , treatment AS treatment  WHERE (((COALESCE(UPPER(diagnosis.stage), '') = UPPER('IIA')) OR (COALESCE(UPPER(diagnosis.stage), '') = UPPER('IIB'))) AND (COALESCE(UPPER(researchsubject.primary_diagnosis_site), '') = UPPER('Lung'))) AND  subject.id = subject_researchsubject.subject_id  AND  subject_researchsubject.researchsubject_id = researchsubject.id  AND  researchsubject.id = researchsubject_diagnosis.researchsubject_id  AND  researchsubject_diagnosis.diagnosis_id = diagnosis.id  AND  subject.id = file_subject.subject_id  AND  file_subject.file_id = file.id  AND  researchsubject.id = researchsubject_specimen.researchsubject_id  AND  researchsubject_specimen.specimen_id = specimen.id  AND  researchsubject.id = researchsubject_treatment.researchsubject_id  AND  researchsubject_treatment.treatment_id = treatment.id  GROUP BY subject.id,subject.species,subject.sex,subject.race,subject.ethnicity,subject.days_to_birth,subject.vital_status,subject.days_to_death,subject.cause_of_death,diagnosis.id,file.id,researchsubject.id,specimen.id,treatment.id) SELECT COUNT(DISTINCT diagnosis_id) AS diagnosis_id_count, COUNT(DISTINCT file_id) AS file_id_count, COUNT(DISTINCT researchsubject_id) AS researchsubject_id_count, COUNT(DISTINCT specimen_id) AS specimen_id_count, COUNT(DISTINCT subject_id) AS subject_id_count, COUNT(DISTINCT treatment_id) AS treatment_id_count FROM flattened_results"));
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
