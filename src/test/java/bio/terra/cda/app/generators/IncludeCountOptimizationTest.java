package bio.terra.cda.app.generators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import bio.terra.cda.app.service.Filter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import bio.terra.cda.app.operators.QueryModule;
import bio.terra.cda.generated.model.Query;

/**
 * IncludeCountOptimizationTest
 */
@Tag("unit")
public class IncludeCountOptimizationTest {

  static final Path TEST_FILES = Paths.get("src/test/resources/query");
  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new QueryModule());
  private final String queryFile = "query-test-primary-disease-site-or-sex-f.json";
  private final String jsonQuery = Files.readString(TEST_FILES.resolve(queryFile));
  private final Query query = objectMapper.readValue(jsonQuery, Query.class);

  public IncludeCountOptimizationTest() throws IOException {
  }

  /**
   * This test will hit the first if statement used for checking for a WHERE
   * statement
   */
  @Test
  void MissingSql() {
    String sqlOg = "";
    EntitySqlGenerator entitySqlGenerator = new EntitySqlGenerator(query, false);
    try {
      Filter filterObj = new Filter(sqlOg, entitySqlGenerator, Boolean.TRUE, "");
      assertThat("This code should have never been hit", equalTo(""));
    } catch (Exception exception) {
      assertThat(exception.getMessage(), equalTo("This query does not contain a where filter"));
    }

  }

  /**
   * Missing where inside of sequel statement
   */
  @Test
  void MissingWhere() {
    String sqlOg = "SELECT count(*) from (SELECT subject.id AS subject_id, subject.species AS species, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.days_to_birth AS days_to_birth, subject.vital_status AS vital_status, subject.days_to_death AS days_to_death, subject.cause_of_death AS cause_of_death, json_agg(distinct (subject_identifier.system, subject_identifier.field_name, subject_identifier.value)::system_data) as subject_identifier, json_agg(distinct subject_associated_project.associated_project) AS subject_associated_project FROM subject AS subject  LEFT JOIN subject_researchsubject AS subject_researchsubject ON subject.integer_id_alias = subject_researchsubject.subject_alias  LEFT JOIN researchsubject AS researchsubject ON subject_researchsubject.researchsubject_alias = researchsubject.integer_id_alias  INNER JOIN subject_identifier AS subject_identifier ON subject.id = subject_identifier.subject_id  INNER JOIN subject_associated_project AS subject_associated_project ON subject.id = subject_associated_project.subject_id    ((COALESCE(UPPER(subject.sex), '') LIKE UPPER(:parameter_1)) OR (COALESCE(UPPER(researchsubject.primary_diagnosis_site), '') LIKE UPPER(:parameter_2))) GROUP BY subject.id,subject.species,subject.sex,subject.race,subject.ethnicity,subject.days_to_birth,subject.vital_status,subject.days_to_death,subject.cause_of_death ORDER BY subject.id asc) as quantify";
    EntitySqlGenerator entitySqlGenerator = new EntitySqlGenerator(query, false);
    try {
      Filter filterObj = new Filter(sqlOg, entitySqlGenerator, Boolean.TRUE, "");
      assertThat("This code should have never been hit", equalTo(""));
    } catch (Exception exception) {
      assertThat(exception.getMessage(), equalTo("This query does not contain a where filter"));
    }

  }

  /**
   * This test the filters Class query optimization
   */
  @Test
  void QueryOptimizationUsingFilterClass() {
    String sqlOg = "SELECT count(*) from (SELECT subject.id AS subject_id, subject.species AS species, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.days_to_birth AS days_to_birth, subject.vital_status AS vital_status, subject.days_to_death AS days_to_death, subject.cause_of_death AS cause_of_death, json_agg(distinct (subject_identifier.system, subject_identifier.field_name, subject_identifier.value)::system_data) as subject_identifier, json_agg(distinct subject_associated_project.associated_project) AS subject_associated_project FROM subject AS subject  LEFT JOIN subject_researchsubject AS subject_researchsubject ON subject.integer_id_alias = subject_researchsubject.subject_alias  LEFT JOIN researchsubject AS researchsubject ON subject_researchsubject.researchsubject_alias = researchsubject.integer_id_alias  INNER JOIN subject_identifier AS subject_identifier ON subject.id = subject_identifier.subject_id  INNER JOIN subject_associated_project AS subject_associated_project ON subject.id = subject_associated_project.subject_id WHERE ((COALESCE(UPPER(subject.sex), '') LIKE UPPER(:parameter_1)) OR (COALESCE(UPPER(researchsubject.primary_diagnosis_site), '') LIKE UPPER(:parameter_2))) GROUP BY subject.id,subject.species,subject.sex,subject.race,subject.ethnicity,subject.days_to_birth,subject.vital_status,subject.days_to_death,subject.cause_of_death ORDER BY subject.id asc) as quantify";
    String expected = "WITH subject_id_preselect_0 AS (SELECT integer_id_alias FROM subject WHERE (COALESCE(UPPER(sex), '') LIKE UPPER(:parameter_1))), researchsubject_id_preselect_1 AS (SELECT integer_id_alias FROM researchsubject WHERE (COALESCE(UPPER(primary_diagnosis_site), '') LIKE UPPER(:parameter_2))), subject_researchsubject_id_preselect_1 AS (SELECT subject_alias FROM subject_researchsubject WHERE researchsubject_alias IN (SELECT integer_id_alias FROM researchsubject_id_preselect_1)) SELECT COUNT(DISTINCT(subject_alias)) FROM (SELECT integer_id_alias AS subject_alias FROM subject_id_preselect_0  UNION  SELECT subject_alias  FROM subject_researchsubject_id_preselect_1) as count_result";
    EntitySqlGenerator entitySqlGenerator = new EntitySqlGenerator(query, false);
    Filter filterObj = new Filter(sqlOg, entitySqlGenerator, Boolean.TRUE, "");
    assertThat(filterObj.getIncludeCountQuery(), equalTo(expected));
  }

  // all public methods need a test

  /**
   *
   * This test will throw a java.lang.StringIndexOutOfBoundsException: String
   * index out of range at (Filter.java:262)
   * Because in the paranthesisSubString Function the string will keep looping,
   * because there is no match found
   * 
   */
  @Test
  void RootSetToFalseThrowsStringIndexOutOfBoundsException() {
    String sqlOg = "SELECT count(*) from (SELECT subject.id AS subject_id, subject.species AS species, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.days_to_birth AS days_to_birth, subject.vital_status AS vital_status, subject.days_to_death AS days_to_death, subject.cause_of_death AS cause_of_death, json_agg(distinct (subject_identifier.system, subject_identifier.field_name, subject_identifier.value)::system_data) as subject_identifier, json_agg(distinct subject_associated_project.associated_project) AS subject_associated_project FROM subject AS subject  LEFT JOIN subject_researchsubject AS subject_researchsubject ON subject.integer_id_alias = subject_researchsubject.subject_alias  LEFT JOIN researchsubject AS researchsubject ON subject_researchsubject.researchsubject_alias = researchsubject.integer_id_alias  INNER JOIN subject_identifier AS subject_identifier ON subject.id = subject_identifier.subject_id  INNER JOIN subject_associated_project AS subject_associated_project ON subject.id = subject_associated_project.subject_id  WHERE  ((COALESCE(UPPER(subject.sex), '') LIKE UPPER(:parameter_1)) OR (COALESCE(UPPER(researchsubject.primary_diagnosis_site), '') LIKE UPPER(:parameter_2))) GROUP BY subject.id,subject.species,subject.sex,subject.race,subject.ethnicity,subject.days_to_birth,subject.vital_status,subject.days_to_death,subject.cause_of_death ORDER BY subject.id asc) as quantify";
    EntitySqlGenerator entitySqlGenerator = new EntitySqlGenerator(query, false);

    assertThrows(StringIndexOutOfBoundsException.class, () -> {
      Filter filterObj = new Filter(sqlOg, entitySqlGenerator, Boolean.FALSE, "");
    });
  }

  /**
   * For this test, I removed the front Parentheses In the WHERE statement
   * this.joinBuilder.getPath
   * Will return a no, because of the value not appearing in the getTableInfo map
   * by default TableInfo tableinfo = null;
   * so this will return null
   */
  @Test
  void FilterContainsParenthesesThrowNullPointerException() {
    String sqlOg = "SELECT count(*) from (SELECT subject.id AS subject_id, subject.species AS species, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.days_to_birth AS days_to_birth, subject.vital_status AS vital_status, subject.days_to_death AS days_to_death, subject.cause_of_death AS cause_of_death, json_agg(distinct (subject_identifier.system, subject_identifier.field_name, subject_identifier.value)::system_data) as subject_identifier, json_agg(distinct subject_associated_project.associated_project) AS subject_associated_project FROM subject AS subject  LEFT JOIN subject_researchsubject AS subject_researchsubject ON subject.integer_id_alias = subject_researchsubject.subject_alias  LEFT JOIN researchsubject AS researchsubject ON subject_researchsubject.researchsubject_alias = researchsubject.integer_id_alias  INNER JOIN subject_identifier AS subject_identifier ON subject.id = subject_identifier.subject_id  INNER JOIN subject_associated_project AS subject_associated_project ON subject.id = subject_associated_project.subject_id  WHERE COALESCE(UPPER(subject.sex))), '') LIKE UPPER(:parameter_1) OR COALESCE(UPPER(researchsubject.primary_diagnosis_site), '') LIKE UPPER(:parameter_2) GROUP BY subject.id,subject.species,subject.sex,subject.race,subject.ethnicity,subject.days_to_birth,subject.vital_status,subject.days_to_death,subject.cause_of_death ORDER BY subject.id asc) as quantify";
    EntitySqlGenerator entitySqlGenerator = new EntitySqlGenerator(query, false);

    assertThrows(NullPointerException.class, () -> {
      Filter filterObj = new Filter(sqlOg, entitySqlGenerator, Boolean.TRUE, "");
    });
  }



}