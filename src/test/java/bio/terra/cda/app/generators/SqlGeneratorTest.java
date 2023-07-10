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
            "SELECT subject.id AS subject_id, subject.species AS species, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.days_to_birth AS days_to_birth, subject.vital_status AS vital_status, subject.days_to_death AS days_to_death, subject.cause_of_death AS cause_of_death FROM subject AS subject WHERE (COALESCE(UPPER(id), '') = UPPER('value')) GROUP BY subject.id,subject.species,subject.sex,subject.race,subject.ethnicity,subject.days_to_birth,subject.vital_status,subject.days_to_death,subject.cause_of_death"),
        Arguments.of(
            "query2.json",
            TABLE,
            TABLE,
            "SELECT subject.id AS subject_id, subject.species AS species, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.days_to_birth AS days_to_birth, subject.vital_status AS vital_status, subject.days_to_death AS days_to_death, subject.cause_of_death AS cause_of_death FROM subject AS subject  LEFT JOIN subject_researchsubject AS subject_researchsubject ON subject.id = subject_researchsubject.subject_id  LEFT JOIN researchsubject AS researchsubject ON subject_researchsubject.researchsubject_id = researchsubject.id  LEFT JOIN researchsubject_specimen AS researchsubject_specimen ON researchsubject.id = researchsubject_specimen.researchsubject_id  LEFT JOIN specimen AS specimen ON researchsubject_specimen.specimen_id = specimen.id WHERE (((COALESCE(UPPER(member_of_research_project), '') >= UPPER('value')) AND (COALESCE(UPPER(specimen_type), '') = UPPER('value'))) AND (COALESCE(UPPER(primary_diagnosis_condition), '') = UPPER('value'))) GROUP BY subject.id,subject.species,subject.sex,subject.race,subject.ethnicity,subject.days_to_birth,subject.vital_status,subject.days_to_death,subject.cause_of_death"),
        Arguments.of(
            "query3.json",
            TABLE,
            TABLE,
            "SELECT subject.id AS subject_id, subject.species AS species, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.days_to_birth AS days_to_birth, subject.vital_status AS vital_status, subject.days_to_death AS days_to_death, subject.cause_of_death AS cause_of_death FROM subject AS subject  LEFT JOIN subject_researchsubject AS subject_researchsubject ON subject.id = subject_researchsubject.subject_id  LEFT JOIN researchsubject AS researchsubject ON subject_researchsubject.researchsubject_id = researchsubject.id  LEFT JOIN researchsubject_specimen AS researchsubject_specimen ON researchsubject.id = researchsubject_specimen.researchsubject_id  LEFT JOIN specimen AS specimen ON researchsubject_specimen.specimen_id = specimen.id WHERE (COALESCE(days_to_collection, :parameter_1) = 50) GROUP BY subject.id,subject.species,subject.sex,subject.race,subject.ethnicity,subject.days_to_birth,subject.vital_status,subject.days_to_death,subject.cause_of_death"),
        Arguments.of(
            "query-subquery.json",
            TABLE,
            TABLE,
            "SELECT subject.id AS subject_id, subject.species AS species, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.days_to_birth AS days_to_birth, subject.vital_status AS vital_status, subject.days_to_death AS days_to_death, subject.cause_of_death AS cause_of_death FROM (SELECT subject.* FROM subject AS subject  LEFT JOIN subject_researchsubject AS subject_researchsubject ON subject.id = subject_researchsubject.subject_id  LEFT JOIN researchsubject AS researchsubject ON subject_researchsubject.researchsubject_id = researchsubject.id  LEFT JOIN researchsubject_identifier AS researchsubject_identifier ON researchsubject.id = researchsubject_identifier.researchsubject_id WHERE (COALESCE(UPPER(system), '') = UPPER('PDC'))) as subject  LEFT JOIN subject_researchsubject AS subject_researchsubject ON subject.id = subject_researchsubject.subject_id  LEFT JOIN researchsubject AS researchsubject ON subject_researchsubject.researchsubject_id = researchsubject.id  LEFT JOIN researchsubject_identifier AS researchsubject_identifier ON researchsubject.id = researchsubject_identifier.researchsubject_id WHERE (COALESCE(UPPER(system), '') = UPPER('GDC')) GROUP BY subject.id,subject.species,subject.sex,subject.race,subject.ethnicity,subject.days_to_birth,subject.vital_status,subject.days_to_death,subject.cause_of_death"),
        Arguments.of(
            "query-not.json",
            TABLE,
            TABLE,
            "SELECT subject.id AS subject_id, subject.species AS species, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.days_to_birth AS days_to_birth, subject.vital_status AS vital_status, subject.days_to_death AS days_to_death, subject.cause_of_death AS cause_of_death FROM subject AS subject  LEFT JOIN subject_researchsubject AS subject_researchsubject ON subject.id = subject_researchsubject.subject_id  LEFT JOIN researchsubject AS researchsubject ON subject_researchsubject.researchsubject_id = researchsubject.id WHERE NOT ((COALESCE(UPPER(primary_diagnosis_condition), '') = UPPER('cancer'))) GROUP BY subject.id,subject.species,subject.sex,subject.race,subject.ethnicity,subject.days_to_birth,subject.vital_status,subject.days_to_death,subject.cause_of_death"),
        Arguments.of(
            "query-ambiguous.json",
            TABLE,
            TABLE,
            "SELECT subject.id AS subject_id, subject.species AS species, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.days_to_birth AS days_to_birth, subject.vital_status AS vital_status, subject.days_to_death AS days_to_death, subject.cause_of_death AS cause_of_death FROM (SELECT subject.* FROM subject AS subject WHERE (COALESCE(UPPER(species), '') = UPPER('dog'))) as subject WHERE (COALESCE(UPPER(species), '') = UPPER('human')) GROUP BY subject.id,subject.species,subject.sex,subject.race,subject.ethnicity,subject.days_to_birth,subject.vital_status,subject.days_to_death,subject.cause_of_death"));
  }

  @ParameterizedTest
  @MethodSource("queryData")
  void testQuery(String queryFile, String qualifiedTable, String table, String expectedQueryFormat)
      throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve(queryFile));

    String expectedSql = String.format(expectedQueryFormat, qualifiedTable, table);

    Query query = objectMapper.readValue(jsonQuery, Query.class);
    String translatedQuery = new SqlGenerator(query, false).getReadableQuerySql();

    assertEquals(expectedSql, translatedQuery);
  }
}
