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

class SqlGeneratorTest {

  static final Path TEST_FILES = Paths.get("src/test/resources/query");

  public static final String TABLE = "subject";
  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new QueryModule());

  private static Stream<Arguments> queryData() {
    return Stream.of(
        Arguments.of(
            "query1.json",
            TABLE,
            TABLE,
            "SELECT subject.id AS subject_id, subject.species AS species, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.days_to_birth AS days_to_birth, subject.vital_status AS vital_status, subject.days_to_death AS days_to_death, subject.cause_of_death AS cause_of_death FROM subject AS subject WHERE (COALESCE(UPPER(subject.id), '') = UPPER('value')) GROUP BY subject.id,subject.species,subject.sex,subject.race,subject.ethnicity,subject.days_to_birth,subject.vital_status,subject.days_to_death,subject.cause_of_death"),
        Arguments.of(
            "query2.json",
            TABLE,
            TABLE,
            "SELECT subject.id AS subject_id, subject.species AS species, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.days_to_birth AS days_to_birth, subject.vital_status AS vital_status, subject.days_to_death AS days_to_death, subject.cause_of_death AS cause_of_death FROM subject AS subject  LEFT JOIN subject_researchsubject AS subject_researchsubject ON subject.integer_id_alias = subject_researchsubject.subject_alias  LEFT JOIN researchsubject AS researchsubject ON subject_researchsubject.researchsubject_alias = researchsubject.integer_id_alias  LEFT JOIN researchsubject_specimen AS researchsubject_specimen ON researchsubject.integer_id_alias = researchsubject_specimen.researchsubject_alias  LEFT JOIN specimen AS specimen ON researchsubject_specimen.specimen_alias = specimen.integer_id_alias WHERE (((COALESCE(UPPER(researchsubject.member_of_research_project), '') >= UPPER('value')) AND (COALESCE(UPPER(specimen.specimen_type), '') = UPPER('value'))) AND (COALESCE(UPPER(researchsubject.primary_diagnosis_condition), '') = UPPER('value'))) GROUP BY subject.id,subject.species,subject.sex,subject.race,subject.ethnicity,subject.days_to_birth,subject.vital_status,subject.days_to_death,subject.cause_of_death"),
        Arguments.of(
            "query3.json",
            TABLE,
            TABLE,
            "SELECT subject.id AS subject_id, subject.species AS species, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.days_to_birth AS days_to_birth, subject.vital_status AS vital_status, subject.days_to_death AS days_to_death, subject.cause_of_death AS cause_of_death FROM subject AS subject  LEFT JOIN subject_researchsubject AS subject_researchsubject ON subject.integer_id_alias = subject_researchsubject.subject_alias  LEFT JOIN researchsubject AS researchsubject ON subject_researchsubject.researchsubject_alias = researchsubject.integer_id_alias  LEFT JOIN researchsubject_specimen AS researchsubject_specimen ON researchsubject.integer_id_alias = researchsubject_specimen.researchsubject_alias  LEFT JOIN specimen AS specimen ON researchsubject_specimen.specimen_alias = specimen.integer_id_alias WHERE (specimen.days_to_collection = 50) GROUP BY subject.id,subject.species,subject.sex,subject.race,subject.ethnicity,subject.days_to_birth,subject.vital_status,subject.days_to_death,subject.cause_of_death"),
        Arguments.of(
            "query-not.json",
            TABLE,
            TABLE,
            "SELECT subject.id AS subject_id, subject.species AS species, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.days_to_birth AS days_to_birth, subject.vital_status AS vital_status, subject.days_to_death AS days_to_death, subject.cause_of_death AS cause_of_death FROM subject AS subject  LEFT JOIN subject_researchsubject AS subject_researchsubject ON subject.integer_id_alias = subject_researchsubject.subject_alias  LEFT JOIN researchsubject AS researchsubject ON subject_researchsubject.researchsubject_alias = researchsubject.integer_id_alias WHERE NOT ((COALESCE(UPPER(researchsubject.primary_diagnosis_condition), '') = UPPER('cancer'))) GROUP BY subject.id,subject.species,subject.sex,subject.race,subject.ethnicity,subject.days_to_birth,subject.vital_status,subject.days_to_death,subject.cause_of_death"));
  }

  @ParameterizedTest
  @MethodSource("queryData")
  void testQuery(String queryFile, String qualifiedTable, String table, String expectedQueryFormat)
      throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve(queryFile));

    String expectedSql = String.format(expectedQueryFormat, qualifiedTable, table);

    Query query = objectMapper.readValue(jsonQuery, Query.class);
    String translatedQuery =
        new EntitySqlGenerator(query, false).getReadableQuerySql();

    assertEquals(expectedSql, translatedQuery);
  }
}
