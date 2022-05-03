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

  public static final String TABLE = "all_v3_0_subjects_meta";
  public static final String QUALIFIED_TABLE = "GROUP." + TABLE;

  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new QueryModule());

  private static Stream<Arguments> queryData() {
    return Stream.of(
        Arguments.of(
            "query1.json",
            QUALIFIED_TABLE,
            TABLE,
            "SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY all_v3_0_subjects_meta.id) as rn, "
                + "all_v3_0_subjects_meta.id AS id, all_v3_0_subjects_meta.identifier AS identifier, "
                + "all_v3_0_subjects_meta.species AS species, all_v3_0_subjects_meta.sex AS sex, "
                + "all_v3_0_subjects_meta.race AS race, all_v3_0_subjects_meta.ethnicity AS ethnicity, "
                + "all_v3_0_subjects_meta.days_to_birth AS days_to_birth, all_v3_0_subjects_meta.subject_associated_project "
                + "AS subject_associated_project, all_v3_0_subjects_meta.vital_status AS vital_status, "
                + "all_v3_0_subjects_meta.age_at_death AS age_at_death, all_v3_0_subjects_meta.cause_of_death AS cause_of_death, "
                + "all_v3_0_subjects_meta.ResearchSubject AS ResearchSubject FROM GROUP.all_v3_0_subjects_meta AS "
                + "all_v3_0_subjects_meta WHERE (UPPER(all_v3_0_subjects_meta.id) = UPPER('value'))) as results WHERE rn = 1"),
        Arguments.of(
            "query2.json",
            QUALIFIED_TABLE,
            TABLE,
            "SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY all_v3_0_subjects_meta.id) as rn, "
                + "all_v3_0_subjects_meta.id AS id, all_v3_0_subjects_meta.identifier AS identifier, "
                + "all_v3_0_subjects_meta.species AS species, all_v3_0_subjects_meta.sex AS sex, all_v3_0_subjects_meta.race AS "
                + "race, all_v3_0_subjects_meta.ethnicity AS ethnicity, all_v3_0_subjects_meta.days_to_birth AS days_to_birth, "
                + "all_v3_0_subjects_meta.subject_associated_project AS subject_associated_project, "
                + "all_v3_0_subjects_meta.vital_status AS vital_status, all_v3_0_subjects_meta.age_at_death AS age_at_death, "
                + "all_v3_0_subjects_meta.cause_of_death AS cause_of_death, all_v3_0_subjects_meta.ResearchSubject AS "
                + "ResearchSubject FROM GROUP.all_v3_0_subjects_meta AS all_v3_0_subjects_meta LEFT JOIN "
                + "UNNEST(all_v3_0_subjects_meta.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.Specimen) "
                + "AS _ResearchSubject_Specimen WHERE (((UPPER(_ResearchSubject.member_of_research_project) >= "
                + "UPPER('value')) AND (UPPER(_ResearchSubject_Specimen.specimen_type) = UPPER('value'))) AND "
                + "(UPPER(_ResearchSubject.primary_diagnosis_condition) = UPPER('value')))) as results WHERE rn = 1"),
        Arguments.of(
            "query3.json",
            QUALIFIED_TABLE,
            TABLE,
            "SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY all_v3_0_subjects_meta.id) as rn, "
                + "all_v3_0_subjects_meta.id AS id, all_v3_0_subjects_meta.identifier AS identifier, all_v3_0_subjects_meta.species "
                + "AS species, all_v3_0_subjects_meta.sex AS sex, all_v3_0_subjects_meta.race AS race, "
                + "all_v3_0_subjects_meta.ethnicity AS ethnicity, all_v3_0_subjects_meta.days_to_birth AS days_to_birth, "
                + "all_v3_0_subjects_meta.subject_associated_project AS subject_associated_project, "
                + "all_v3_0_subjects_meta.vital_status AS vital_status, all_v3_0_subjects_meta.age_at_death AS age_at_death, "
                + "all_v3_0_subjects_meta.cause_of_death AS cause_of_death, all_v3_0_subjects_meta.ResearchSubject AS "
                + "ResearchSubject FROM GROUP.all_v3_0_subjects_meta AS all_v3_0_subjects_meta LEFT JOIN "
                + "UNNEST(all_v3_0_subjects_meta.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.Specimen) "
                + "AS _ResearchSubject_Specimen WHERE (_ResearchSubject_Specimen.age_at_collection = 50)) as results WHERE rn = 1"),
        Arguments.of(
            "query-subquery.json",
            "GROUP.all_v3_0_subjects_meta",
            "all_v3_0_subjects_meta",
            "SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY all_v3_0_subjects_meta.id) as rn, "
                + "all_v3_0_subjects_meta.id AS id, all_v3_0_subjects_meta.identifier AS identifier, all_v3_0_subjects_meta.species "
                + "AS species, all_v3_0_subjects_meta.sex AS sex, all_v3_0_subjects_meta.race AS race, "
                + "all_v3_0_subjects_meta.ethnicity AS ethnicity, all_v3_0_subjects_meta.days_to_birth AS days_to_birth, "
                + "all_v3_0_subjects_meta.subject_associated_project AS subject_associated_project, "
                + "all_v3_0_subjects_meta.vital_status AS vital_status, all_v3_0_subjects_meta.age_at_death AS age_at_death, "
                + "all_v3_0_subjects_meta.cause_of_death AS cause_of_death, all_v3_0_subjects_meta.ResearchSubject AS "
                + "ResearchSubject FROM (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY "
                + "all_v3_0_subjects_meta.id) as rn, all_v3_0_subjects_meta.* FROM GROUP.all_v3_0_subjects_meta AS "
                + "all_v3_0_subjects_meta LEFT JOIN UNNEST(all_v3_0_subjects_meta.ResearchSubject) AS _ResearchSubject LEFT JOIN "
                + "UNNEST(_ResearchSubject.identifier) AS _ResearchSubject_identifier WHERE "
                + "(UPPER(_ResearchSubject_identifier.system) = UPPER('PDC'))) as results WHERE rn = 1) AS all_v3_0_subjects_meta "
                + "LEFT JOIN UNNEST(all_v3_0_subjects_meta.ResearchSubject) AS _ResearchSubject LEFT JOIN "
                + "UNNEST(_ResearchSubject.identifier) AS _ResearchSubject_identifier WHERE "
                + "(UPPER(_ResearchSubject_identifier.system) = UPPER('GDC'))) as results WHERE rn = 1"),
        Arguments.of(
            "query-not.json",
            QUALIFIED_TABLE,
            TABLE,
            "SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY all_v3_0_subjects_meta.id) as rn, "
                + "all_v3_0_subjects_meta.id AS id, all_v3_0_subjects_meta.identifier AS identifier, all_v3_0_subjects_meta.species "
                + "AS species, all_v3_0_subjects_meta.sex AS sex, all_v3_0_subjects_meta.race AS race, "
                + "all_v3_0_subjects_meta.ethnicity AS ethnicity, all_v3_0_subjects_meta.days_to_birth AS days_to_birth, "
                + "all_v3_0_subjects_meta.subject_associated_project AS subject_associated_project, "
                + "all_v3_0_subjects_meta.vital_status AS vital_status, all_v3_0_subjects_meta.age_at_death AS age_at_death, "
                + "all_v3_0_subjects_meta.cause_of_death AS cause_of_death, all_v3_0_subjects_meta.ResearchSubject AS "
                + "ResearchSubject FROM GROUP.all_v3_0_subjects_meta AS all_v3_0_subjects_meta LEFT JOIN "
                + "UNNEST(all_v3_0_subjects_meta.ResearchSubject) AS _ResearchSubject WHERE (NOT (UPPER('cancer') = "
                + "UPPER(_ResearchSubject.primary_diagnosis_condition)))) as results WHERE rn = 1"),
        Arguments.of(
            "query-ambiguous.json",
            QUALIFIED_TABLE,
            TABLE,
            "SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY all_v3_0_subjects_meta.id) as rn, "
                + "all_v3_0_subjects_meta.id AS id, all_v3_0_subjects_meta.identifier AS identifier, all_v3_0_subjects_meta.species "
                + "AS species, all_v3_0_subjects_meta.sex AS sex, all_v3_0_subjects_meta.race AS race, "
                + "all_v3_0_subjects_meta.ethnicity AS ethnicity, all_v3_0_subjects_meta.days_to_birth AS days_to_birth, "
                + "all_v3_0_subjects_meta.subject_associated_project AS subject_associated_project, "
                + "all_v3_0_subjects_meta.vital_status AS vital_status, all_v3_0_subjects_meta.age_at_death AS age_at_death, "
                + "all_v3_0_subjects_meta.cause_of_death AS cause_of_death, all_v3_0_subjects_meta.ResearchSubject AS "
                + "ResearchSubject FROM (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY "
                + "all_v3_0_subjects_meta.id) as rn, all_v3_0_subjects_meta.* FROM GROUP.all_v3_0_subjects_meta AS "
                + "all_v3_0_subjects_meta WHERE (UPPER(all_v3_0_subjects_meta.species) = UPPER('dog'))) as results WHERE rn = 1) "
                + "AS all_v3_0_subjects_meta WHERE (UPPER(all_v3_0_subjects_meta.species) = UPPER('human'))) as results WHERE rn = 1"));
  }

  @ParameterizedTest
  @MethodSource("queryData")
  void testQuery(String queryFile, String qualifiedTable, String table, String expectedQueryFormat)
      throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve(queryFile));

    String expectedSql = String.format(expectedQueryFormat, qualifiedTable, table);

    Query query = objectMapper.readValue(jsonQuery, Query.class);
    String translatedQuery = new SqlGenerator(qualifiedTable, query, table).generate();

    assertEquals(expectedSql, translatedQuery);
  }
}
