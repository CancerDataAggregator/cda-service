package bio.terra.cda.app.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;

import bio.terra.cda.app.operators.QueryModule;
import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class EntityCountSqlGeneratorTest {

  static final Path TEST_FILES = Paths.get("src/test/resources/query");

  public static final String TABLE = "all_Subjects_v3_0_final";
  public static final String QUALIFIED_TABLE = "gdc-bq-sample.dev." + TABLE;

  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new QueryModule());

  public static Stream<Arguments> queryData() {
    return Stream.of(
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            ResearchSubjectCountSqlGenerator.class,
            "WITH flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY _ResearchSubject.id, _researchsubject_identifier.system) as rn, _ResearchSubject.id AS researchsubject_id, _ResearchSubject.primary_diagnosis_condition AS primary_diagnosis_condition, _ResearchSubject.primary_diagnosis_site AS primary_diagnosis_site, _researchsubject_identifier.system AS researchsubject_identifier_system FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject INNER JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis LEFT JOIN UNNEST(_ResearchSubject.identifier) AS _researchsubject_identifier WHERE (((IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@parameter_1)) OR (IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@parameter_2))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@parameter_3)))) as results WHERE rn = 1 ) select (SELECT COUNT(DISTINCT researchsubject_id) from flattened_result) as total, (select ARRAY(select as STRUCT primary_diagnosis_condition, count(distinct researchsubject_id) as count from flattened_result group by primary_diagnosis_condition)) as primary_diagnosis_condition, (select ARRAY(select as STRUCT primary_diagnosis_site, count(distinct researchsubject_id) as count from flattened_result group by primary_diagnosis_site)) as primary_diagnosis_site, (select ARRAY(select as STRUCT researchsubject_identifier_system, count(distinct researchsubject_id) as count from flattened_result group by researchsubject_identifier_system)) as researchsubject_identifier_system"),
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            SubjectCountSqlGenerator.class,
        "WITH flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY Subject.id, _subject_identifier.system) as rn, Subject.id AS subject_id, Subject.sex AS sex, Subject.race AS race, Subject.ethnicity AS ethnicity, Subject.cause_of_death AS cause_of_death, _subject_identifier.system AS subject_identifier_system FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject LEFT JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis LEFT JOIN UNNEST(Subject.identifier) AS _subject_identifier WHERE (((IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@parameter_1)) OR (IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@parameter_2))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@parameter_3)))) as results WHERE rn = 1 ) select (SELECT COUNT(DISTINCT subject_id) from flattened_result) as total, (select ARRAY(select as STRUCT sex, count(distinct subject_id) as count from flattened_result group by sex)) as sex, (select ARRAY(select as STRUCT race, count(distinct subject_id) as count from flattened_result group by race)) as race, (select ARRAY(select as STRUCT ethnicity, count(distinct subject_id) as count from flattened_result group by ethnicity)) as ethnicity, (select ARRAY(select as STRUCT cause_of_death, count(distinct subject_id) as count from flattened_result group by cause_of_death)) as cause_of_death, (select ARRAY(select as STRUCT subject_identifier_system, count(distinct subject_id) as count from flattened_result group by subject_identifier_system)) as subject_identifier_system"),
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            SpecimenCountSqlGenerator.class,
        "WITH flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY _Specimen.id, _specimen_identifier.system) as rn, _Specimen.id AS specimen_id, _Specimen.primary_disease_type AS primary_disease_type, _Specimen.source_material_type AS source_material_type, _Specimen.specimen_type AS specimen_type, _specimen_identifier.system AS specimen_identifier_system FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject INNER JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject INNER JOIN UNNEST(_ResearchSubject.Specimen) AS _Specimen LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis LEFT JOIN UNNEST(_Specimen.identifier) AS _specimen_identifier WHERE (((IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@parameter_1)) OR (IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@parameter_2))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@parameter_3)))) as results WHERE rn = 1 ) select (SELECT COUNT(DISTINCT specimen_id) from flattened_result) as total, (select ARRAY(select as STRUCT primary_disease_type, count(distinct specimen_id) as count from flattened_result group by primary_disease_type)) as primary_disease_type, (select ARRAY(select as STRUCT source_material_type, count(distinct specimen_id) as count from flattened_result group by source_material_type)) as source_material_type, (select ARRAY(select as STRUCT specimen_type, count(distinct specimen_id) as count from flattened_result group by specimen_type)) as specimen_type, (select ARRAY(select as STRUCT specimen_identifier_system, count(distinct specimen_id) as count from flattened_result group by specimen_identifier_system)) as specimen_identifier_system"),
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            DiagnosisCountSqlGenerator.class,
        "WITH flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY _Diagnosis.id, _diagnosis_identifier.system) as rn, _Diagnosis.id AS diagnosis_id, _Diagnosis.primary_diagnosis AS primary_diagnosis, _Diagnosis.stage AS stage, _Diagnosis.grade AS grade, _diagnosis_identifier.system AS diagnosis_identifier_system FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject INNER JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject INNER JOIN UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis LEFT JOIN UNNEST(_Diagnosis.identifier) AS _diagnosis_identifier WHERE (((IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@parameter_1)) OR (IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@parameter_2))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@parameter_3)))) as results WHERE rn = 1 ) select (SELECT COUNT(DISTINCT diagnosis_id) from flattened_result) as total, (select ARRAY(select as STRUCT primary_diagnosis, count(distinct diagnosis_id) as count from flattened_result group by primary_diagnosis)) as primary_diagnosis, (select ARRAY(select as STRUCT stage, count(distinct diagnosis_id) as count from flattened_result group by stage)) as stage, (select ARRAY(select as STRUCT grade, count(distinct diagnosis_id) as count from flattened_result group by grade)) as grade, (select ARRAY(select as STRUCT diagnosis_identifier_system, count(distinct diagnosis_id) as count from flattened_result group by diagnosis_identifier_system)) as diagnosis_identifier_system"),
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            TreatmentCountSqlGenerator.class,
            "WITH flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY _Treatment.id, _treatment_identifier.system) as rn, _Treatment.id AS treatment_id, _Treatment.treatment_type AS treatment_type, _Treatment.treatment_effect AS treatment_effect, _treatment_identifier.system AS treatment_identifier_system FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject INNER JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject INNER JOIN UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis INNER JOIN UNNEST(_Diagnosis.Treatment) AS _Treatment LEFT JOIN UNNEST(_Treatment.identifier) AS _treatment_identifier WHERE (((IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@parameter_1)) OR (IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@parameter_2))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@parameter_3)))) as results WHERE rn = 1 ) select (SELECT COUNT(DISTINCT treatment_id) from flattened_result) as total, (select ARRAY(select as STRUCT treatment_type, count(distinct treatment_id) as count from flattened_result group by treatment_type)) as treatment_type, (select ARRAY(select as STRUCT treatment_effect, count(distinct treatment_id) as count from flattened_result group by treatment_effect)) as treatment_effect, (select ARRAY(select as STRUCT treatment_identifier_system, count(distinct treatment_id) as count from flattened_result group by treatment_identifier_system)) as treatment_identifier_system"),
        Arguments.of(
            "query-file.json",
            QUALIFIED_TABLE,
            TABLE,
            SubjectCountSqlGenerator.class,
                "WITH flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY Subject.id, _subject_identifier.system) as rn, Subject.id AS subject_id, Subject.sex AS sex, Subject.race AS race, Subject.ethnicity AS ethnicity, Subject.cause_of_death AS cause_of_death, _subject_identifier.system AS subject_identifier_system FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject LEFT JOIN UNNEST(Subject.Files) AS _subject_Files LEFT JOIN gdc-bq-sample.dev.all_Files_v3_0_final AS File ON _subject_Files = File.id LEFT JOIN UNNEST(Subject.identifier) AS _subject_identifier WHERE (IFNULL(UPPER(File.data_modality), '') = UPPER(@parameter_1))) as results WHERE rn = 1 ) select (SELECT COUNT(DISTINCT subject_id) from flattened_result) as total, (select ARRAY(select as STRUCT sex, count(distinct subject_id) as count from flattened_result group by sex)) as sex, (select ARRAY(select as STRUCT race, count(distinct subject_id) as count from flattened_result group by race)) as race, (select ARRAY(select as STRUCT ethnicity, count(distinct subject_id) as count from flattened_result group by ethnicity)) as ethnicity, (select ARRAY(select as STRUCT cause_of_death, count(distinct subject_id) as count from flattened_result group by cause_of_death)) as cause_of_death, (select ARRAY(select as STRUCT subject_identifier_system, count(distinct subject_id) as count from flattened_result group by subject_identifier_system)) as subject_identifier_system"));
  }

  @ParameterizedTest
  @MethodSource("queryData")
  void testQuery(
      String queryFile,
      String qualifiedTable,
      String table,
      Class<? extends EntityCountSqlGenerator> clazz,
      String expectedQueryFormat)
      throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve(queryFile));
    String expectedSql = String.format(expectedQueryFormat, qualifiedTable, table);

    Query query = objectMapper.readValue(jsonQuery, Query.class);
    Constructor<? extends EntityCountSqlGenerator> ct =
        clazz.getDeclaredConstructor(String.class, Query.class, String.class);
    String translatedQuery =
        ct.newInstance(qualifiedTable, query, table).generate().build().getQuery();

    assertEquals(expectedSql, translatedQuery);
  }
}
