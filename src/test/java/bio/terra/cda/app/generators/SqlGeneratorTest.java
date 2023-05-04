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

  public static final String TABLE = "all_Subjects_v3_0_final";
  public static final String QUALIFIED_TABLE = "GROUP." + TABLE;

  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new QueryModule());

  private static Stream<Arguments> queryData() {
    return Stream.of(
        Arguments.of(
            "query1.json",
            QUALIFIED_TABLE,
            TABLE,
            "SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY Subject.id) as rn, Subject.id AS subject_id, Subject.identifier AS subject_identifier, Subject.species AS species, Subject.sex AS sex, Subject.race AS race, Subject.ethnicity AS ethnicity, Subject.days_to_birth AS days_to_birth, Subject.subject_associated_project AS subject_associated_project, Subject.vital_status AS vital_status, Subject.days_to_death AS days_to_death, Subject.cause_of_death AS cause_of_death, Subject.Files AS subject_Files, Subject.ResearchSubject AS ResearchSubject FROM GROUP.all_Subjects_v3_0_final AS Subject WHERE (IFNULL(UPPER(Subject.id), '') = UPPER(@parameter_1))) as results WHERE rn = 1 "),
        Arguments.of(
            "query2.json",
            QUALIFIED_TABLE,
            TABLE,
            "SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY Subject.id) as rn, Subject.id AS subject_id, Subject.identifier AS subject_identifier, Subject.species AS species, Subject.sex AS sex, Subject.race AS race, Subject.ethnicity AS ethnicity, Subject.days_to_birth AS days_to_birth, Subject.subject_associated_project AS subject_associated_project, Subject.vital_status AS vital_status, Subject.days_to_death AS days_to_death, Subject.cause_of_death AS cause_of_death, Subject.Files AS subject_Files, Subject.ResearchSubject AS ResearchSubject FROM GROUP.all_Subjects_v3_0_final AS Subject LEFT JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.Specimen) AS _Specimen WHERE (((IFNULL(UPPER(_ResearchSubject.member_of_research_project), '') >= UPPER(@parameter_1)) AND (IFNULL(UPPER(_Specimen.specimen_type), '') = UPPER(@parameter_2))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_condition), '') = UPPER(@parameter_3)))) as results WHERE rn = 1 "),
        Arguments.of(
            "query3.json",
            QUALIFIED_TABLE,
            TABLE,
            "SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY Subject.id) as rn, Subject.id AS subject_id, Subject.identifier AS subject_identifier, Subject.species AS species, Subject.sex AS sex, Subject.race AS race, Subject.ethnicity AS ethnicity, Subject.days_to_birth AS days_to_birth, Subject.subject_associated_project AS subject_associated_project, Subject.vital_status AS vital_status, Subject.days_to_death AS days_to_death, Subject.cause_of_death AS cause_of_death, Subject.Files AS subject_Files, Subject.ResearchSubject AS ResearchSubject FROM GROUP.all_Subjects_v3_0_final AS Subject LEFT JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.Specimen) AS _Specimen WHERE (_Specimen.days_to_collection = @parameter_1)) as results WHERE rn = 1 "),
        Arguments.of(
            "query-subquery.json",
            QUALIFIED_TABLE,
            TABLE,
            "SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY Subject.id) as rn, Subject.id AS subject_id, Subject.identifier AS subject_identifier, Subject.species AS species, Subject.sex AS sex, Subject.race AS race, Subject.ethnicity AS ethnicity, Subject.days_to_birth AS days_to_birth, Subject.subject_associated_project AS subject_associated_project, Subject.vital_status AS vital_status, Subject.days_to_death AS days_to_death, Subject.cause_of_death AS cause_of_death, Subject.Files AS subject_Files, Subject.ResearchSubject AS ResearchSubject FROM (SELECT Subject.* FROM GROUP.all_Subjects_v3_0_final AS Subject LEFT JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.identifier) AS _researchsubject_identifier WHERE (IFNULL(UPPER(_researchsubject_identifier.system), '') = UPPER(@parameter_1))) as Subject LEFT JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.identifier) AS _researchsubject_identifier WHERE (IFNULL(UPPER(_researchsubject_identifier.system), '') = UPPER(@parameter_2))) as results WHERE rn = 1 "),
        Arguments.of(
            "query-not.json",
            QUALIFIED_TABLE,
            TABLE,
            "SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY Subject.id) as rn, Subject.id AS subject_id, Subject.identifier AS subject_identifier, Subject.species AS species, Subject.sex AS sex, Subject.race AS race, Subject.ethnicity AS ethnicity, Subject.days_to_birth AS days_to_birth, Subject.subject_associated_project AS subject_associated_project, Subject.vital_status AS vital_status, Subject.days_to_death AS days_to_death, Subject.cause_of_death AS cause_of_death, Subject.Files AS subject_Files, Subject.ResearchSubject AS ResearchSubject FROM GROUP.all_Subjects_v3_0_final AS Subject LEFT JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject WHERE NOT ((IFNULL(UPPER(_ResearchSubject.primary_diagnosis_condition), '') = UPPER(@parameter_1)))) as results WHERE rn = 1 "),
        Arguments.of(
            "query-ambiguous.json",
            QUALIFIED_TABLE,
            TABLE,
            "SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY Subject.id) as rn, Subject.id AS subject_id, Subject.identifier AS subject_identifier, Subject.species AS species, Subject.sex AS sex, Subject.race AS race, Subject.ethnicity AS ethnicity, Subject.days_to_birth AS days_to_birth, Subject.subject_associated_project AS subject_associated_project, Subject.vital_status AS vital_status, Subject.days_to_death AS days_to_death, Subject.cause_of_death AS cause_of_death, Subject.Files AS subject_Files, Subject.ResearchSubject AS ResearchSubject FROM (SELECT Subject.* FROM GROUP.all_Subjects_v3_0_final AS Subject WHERE (IFNULL(UPPER(Subject.species), '') = UPPER(@parameter_1))) as Subject WHERE (IFNULL(UPPER(Subject.species), '') = UPPER(@parameter_2))) as results WHERE rn = 1 "));
  }

  @ParameterizedTest
  @MethodSource("queryData")
  void testQuery(String queryFile, String qualifiedTable, String table, String expectedQueryFormat)
      throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve(queryFile));

    String expectedSql = String.format(expectedQueryFormat, qualifiedTable, table);

    Query query = objectMapper.readValue(jsonQuery, Query.class);
    String translatedQuery =
        new SqlGenerator(qualifiedTable, query, table, false).generate().build().getQuery();

    assertEquals(expectedSql, translatedQuery);
  }
}
