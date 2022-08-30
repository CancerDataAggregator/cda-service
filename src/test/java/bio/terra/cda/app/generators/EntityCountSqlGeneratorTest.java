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
            "with flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY _ResearchSubject.id, _researchsubject_identifier.system, _researchsubject_Files) as rn, _ResearchSubject.id AS researchsubject_id, _researchsubject_identifier.system AS researchsubject_identifier_system, _researchsubject_identifier.value AS researchsubject_identifier_value, _ResearchSubject.member_of_research_project AS member_of_research_project, _ResearchSubject.primary_diagnosis_condition AS primary_diagnosis_condition, _ResearchSubject.primary_diagnosis_site AS primary_diagnosis_site, _researchsubject_Files FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject INNER JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis LEFT JOIN UNNEST(_ResearchSubject.identifier) AS _researchsubject_identifier LEFT JOIN UNNEST(_ResearchSubject.Files) AS _researchsubject_Files WHERE (((IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_1)) OR (IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_2))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@primary_diagnosis_site_1)))) as results WHERE rn = 1) select (SELECT COUNT(DISTINCT researchsubject_id) from flattened_result) as total, (SELECT COUNT(DISTINCT _researchsubject_Files) from flattened_result) as files, (select ARRAY(select as STRUCT researchsubject_identifier_system, count(distinct researchsubject_id) as count from flattened_result group by researchsubject_identifier_system)) as researchsubject_identifier_system, (select ARRAY(select as STRUCT primary_diagnosis_condition, count(distinct researchsubject_id) as count from flattened_result group by primary_diagnosis_condition)) as primary_diagnosis_condition, (select ARRAY(select as STRUCT primary_diagnosis_site, count(distinct researchsubject_id) as count from flattened_result group by primary_diagnosis_site)) as primary_diagnosis_site"),
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            SubjectCountSqlGenerator.class,
            "with flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY Subject.id, _subject_identifier.system, _subject_associated_project, _subject_Files) as rn, Subject.id AS subject_id, _subject_identifier.system AS subject_identifier_system, _subject_identifier.value AS subject_identifier_value, Subject.species AS species, Subject.sex AS sex, Subject.race AS race, Subject.ethnicity AS ethnicity, Subject.days_to_birth AS days_to_birth, _subject_associated_project, Subject.vital_status AS vital_status, Subject.days_to_death AS days_to_death, Subject.cause_of_death AS cause_of_death, _subject_Files FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject LEFT JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis LEFT JOIN UNNEST(Subject.identifier) AS _subject_identifier LEFT JOIN UNNEST(Subject.subject_associated_project) AS _subject_associated_project LEFT JOIN UNNEST(Subject.Files) AS _subject_Files WHERE (((IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_1)) OR (IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_2))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@primary_diagnosis_site_1)))) as results WHERE rn = 1) select (SELECT COUNT(DISTINCT subject_id) from flattened_result) as total, (SELECT COUNT(DISTINCT _subject_Files) from flattened_result) as files, (select ARRAY(select as STRUCT subject_identifier_system, count(distinct subject_id) as count from flattened_result group by subject_identifier_system)) as subject_identifier_system, (select ARRAY(select as STRUCT sex, count(distinct subject_id) as count from flattened_result group by sex)) as sex, (select ARRAY(select as STRUCT race, count(distinct subject_id) as count from flattened_result group by race)) as race, (select ARRAY(select as STRUCT ethnicity, count(distinct subject_id) as count from flattened_result group by ethnicity)) as ethnicity, (select ARRAY(select as STRUCT cause_of_death, count(distinct subject_id) as count from flattened_result group by cause_of_death)) as cause_of_death"),
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            SpecimenCountSqlGenerator.class,
            "with flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY _Specimen.id, _specimen_identifier.system, _specimen_Files) as rn, _Specimen.id AS specimen_id, _specimen_identifier.system AS specimen_identifier_system, _specimen_identifier.value AS specimen_identifier_value, _Specimen.associated_project AS specimen_associated_project, _Specimen.days_to_collection AS days_to_collection, _Specimen.primary_disease_type AS primary_disease_type, _Specimen.anatomical_site AS anatomical_site, _Specimen.source_material_type AS source_material_type, _Specimen.specimen_type AS specimen_type, _Specimen.derived_from_specimen AS derived_from_specimen, _Specimen.derived_from_subject AS derived_from_subject, _specimen_Files FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject INNER JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject INNER JOIN UNNEST(_ResearchSubject.Specimen) AS _Specimen LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis LEFT JOIN UNNEST(_Specimen.identifier) AS _specimen_identifier LEFT JOIN UNNEST(_Specimen.Files) AS _specimen_Files WHERE (((IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_1)) OR (IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_2))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@primary_diagnosis_site_1)))) as results WHERE rn = 1) select (SELECT COUNT(DISTINCT specimen_id) from flattened_result) as total, (SELECT COUNT(DISTINCT _specimen_Files) from flattened_result) as files, (select ARRAY(select as STRUCT specimen_identifier_system, count(distinct specimen_id) as count from flattened_result group by specimen_identifier_system)) as specimen_identifier_system, (select ARRAY(select as STRUCT primary_disease_type, count(distinct specimen_id) as count from flattened_result group by primary_disease_type)) as primary_disease_type, (select ARRAY(select as STRUCT source_material_type, count(distinct specimen_id) as count from flattened_result group by source_material_type)) as source_material_type, (select ARRAY(select as STRUCT specimen_type, count(distinct specimen_id) as count from flattened_result group by specimen_type)) as specimen_type"),
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            DiagnosisCountSqlGenerator.class,
            "with flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY _Diagnosis.id, _diagnosis_identifier.system) as rn, _Diagnosis.id AS diagnosis_id, _diagnosis_identifier.system AS diagnosis_identifier_system, _diagnosis_identifier.value AS diagnosis_identifier_value, _Diagnosis.primary_diagnosis AS primary_diagnosis, _Diagnosis.age_at_diagnosis AS age_at_diagnosis, _Diagnosis.morphology AS morphology, _Diagnosis.stage AS stage, _Diagnosis.grade AS grade, _Diagnosis.method_of_diagnosis AS method_of_diagnosis FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject INNER JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject INNER JOIN UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis LEFT JOIN UNNEST(_Diagnosis.identifier) AS _diagnosis_identifier WHERE (((IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_1)) OR (IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_2))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@primary_diagnosis_site_1)))) as results WHERE rn = 1) select (SELECT COUNT(DISTINCT diagnosis_id) from flattened_result) as total, (select ARRAY(select as STRUCT diagnosis_identifier_system, count(distinct diagnosis_id) as count from flattened_result group by diagnosis_identifier_system)) as diagnosis_identifier_system, (select ARRAY(select as STRUCT primary_diagnosis, count(distinct diagnosis_id) as count from flattened_result group by primary_diagnosis)) as primary_diagnosis, (select ARRAY(select as STRUCT stage, count(distinct diagnosis_id) as count from flattened_result group by stage)) as stage, (select ARRAY(select as STRUCT grade, count(distinct diagnosis_id) as count from flattened_result group by grade)) as grade"),
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            TreatmentCountSqlGenerator.class,
            "with flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY _Treatment.id, _treatment_identifier.system) as rn, _Treatment.id AS treatment_id, _treatment_identifier.system AS treatment_identifier_system, _treatment_identifier.value AS treatment_identifier_value, _Treatment.treatment_type AS treatment_type, _Treatment.treatment_outcome AS treatment_outcome, _Treatment.days_to_treatment_start AS days_to_treatment_start, _Treatment.days_to_treatment_end AS days_to_treatment_end, _Treatment.therapeutic_agent AS therapeutic_agent, _Treatment.treatment_anatomic_site AS treatment_anatomic_site, _Treatment.treatment_effect AS treatment_effect, _Treatment.treatment_end_reason AS treatment_end_reason, _Treatment.number_of_cycles AS number_of_cycles FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject INNER JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject INNER JOIN UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis INNER JOIN UNNEST(_Diagnosis.Treatment) AS _Treatment LEFT JOIN UNNEST(_Treatment.identifier) AS _treatment_identifier WHERE (((IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_1)) OR (IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_2))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@primary_diagnosis_site_1)))) as results WHERE rn = 1) select (SELECT COUNT(DISTINCT treatment_id) from flattened_result) as total, (select ARRAY(select as STRUCT treatment_identifier_system, count(distinct treatment_id) as count from flattened_result group by treatment_identifier_system)) as treatment_identifier_system, (select ARRAY(select as STRUCT treatment_type, count(distinct treatment_id) as count from flattened_result group by treatment_type)) as treatment_type, (select ARRAY(select as STRUCT treatment_effect, count(distinct treatment_id) as count from flattened_result group by treatment_effect)) as treatment_effect"),
        Arguments.of(
            "query-file.json",
            QUALIFIED_TABLE,
            TABLE,
            SubjectCountSqlGenerator.class,
            "with flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY Subject.id, _subject_identifier.system, _subject_associated_project, _subject_Files) as rn, Subject.id AS subject_id, _subject_identifier.system AS subject_identifier_system, _subject_identifier.value AS subject_identifier_value, Subject.species AS species, Subject.sex AS sex, Subject.race AS race, Subject.ethnicity AS ethnicity, Subject.days_to_birth AS days_to_birth, _subject_associated_project, Subject.vital_status AS vital_status, Subject.days_to_death AS days_to_death, Subject.cause_of_death AS cause_of_death, _subject_Files FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject LEFT JOIN UNNEST(Subject.Files) AS _subject_Files LEFT JOIN gdc-bq-sample.dev.all_Files_v3_0_final AS File ON _subject_Files = File.id LEFT JOIN UNNEST(Subject.identifier) AS _subject_identifier LEFT JOIN UNNEST(Subject.subject_associated_project) AS _subject_associated_project WHERE (IFNULL(UPPER(File.data_modality), '') = UPPER(@data_modality_1))) as results WHERE rn = 1) select (SELECT COUNT(DISTINCT subject_id) from flattened_result) as total, (SELECT COUNT(DISTINCT _subject_Files) from flattened_result) as files, (select ARRAY(select as STRUCT subject_identifier_system, count(distinct subject_id) as count from flattened_result group by subject_identifier_system)) as subject_identifier_system, (select ARRAY(select as STRUCT sex, count(distinct subject_id) as count from flattened_result group by sex)) as sex, (select ARRAY(select as STRUCT race, count(distinct subject_id) as count from flattened_result group by race)) as race, (select ARRAY(select as STRUCT ethnicity, count(distinct subject_id) as count from flattened_result group by ethnicity)) as ethnicity, (select ARRAY(select as STRUCT cause_of_death, count(distinct subject_id) as count from flattened_result group by cause_of_death)) as cause_of_death"));
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
