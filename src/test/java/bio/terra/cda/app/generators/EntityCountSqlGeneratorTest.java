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

  public static final String TABLE = "all_Subjects_v3_0_w_RS";
  public static final String QUALIFIED_TABLE = "gdc-bq-sample.dev." + TABLE;

  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new QueryModule());

  public static Stream<Arguments> queryData() {
    return Stream.of(
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            ResearchSubjectCountSqlGenerator.class,
            "with flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY _ResearchSubject.id, _ResearchSubject_identifier.system, _ResearchSubject_Files) as rn, _ResearchSubject.id AS id, _ResearchSubject_identifier.system AS system, _ResearchSubject_identifier.value AS value, _ResearchSubject.member_of_research_project AS member_of_research_project, _ResearchSubject.primary_diagnosis_condition AS primary_diagnosis_condition, _ResearchSubject.primary_diagnosis_site AS primary_diagnosis_site, _ResearchSubject_Files FROM gdc-bq-sample.dev.all_Subjects_v3_0_w_RS AS all_Subjects_v3_0_w_RS INNER JOIN UNNEST(all_Subjects_v3_0_w_RS.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _ResearchSubject_Diagnosis INNER JOIN UNNEST(_ResearchSubject.identifier) AS _ResearchSubject_identifier LEFT JOIN UNNEST(_ResearchSubject.Files) AS _ResearchSubject_Files WHERE (((IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER('Stage I')) OR (IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER('Stage II'))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER('Kidney')))) as results WHERE rn = 1) select (SELECT COUNT(DISTINCT id) from flattened_result) as total, (SELECT COUNT(DISTINCT _ResearchSubject_Files) from flattened_result) as files, (select ARRAY(select as STRUCT system, count(distinct id) as count from flattened_result group by system)) as system, (select ARRAY(select as STRUCT primary_diagnosis_condition, count(distinct id) as count from flattened_result group by primary_diagnosis_condition)) as primary_diagnosis_condition, (select ARRAY(select as STRUCT primary_diagnosis_site, count(distinct id) as count from flattened_result group by primary_diagnosis_site)) as primary_diagnosis_site"),
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            SubjectCountSqlGenerator.class,
            "with flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY all_Subjects_v3_0_w_RS.id, _identifier.system, _subject_associated_project, _Files) as rn, all_Subjects_v3_0_w_RS.id AS id, _identifier.system AS system, _identifier.value AS value, all_Subjects_v3_0_w_RS.species AS species, all_Subjects_v3_0_w_RS.sex AS sex, all_Subjects_v3_0_w_RS.race AS race, all_Subjects_v3_0_w_RS.ethnicity AS ethnicity, all_Subjects_v3_0_w_RS.days_to_birth AS days_to_birth, _subject_associated_project, all_Subjects_v3_0_w_RS.vital_status AS vital_status, all_Subjects_v3_0_w_RS.age_at_death AS age_at_death, all_Subjects_v3_0_w_RS.cause_of_death AS cause_of_death, _Files FROM gdc-bq-sample.dev.all_Subjects_v3_0_w_RS AS all_Subjects_v3_0_w_RS LEFT JOIN UNNEST(all_Subjects_v3_0_w_RS.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _ResearchSubject_Diagnosis INNER JOIN UNNEST(all_Subjects_v3_0_w_RS.identifier) AS _identifier INNER JOIN UNNEST(all_Subjects_v3_0_w_RS.subject_associated_project) AS _subject_associated_project LEFT JOIN UNNEST(all_Subjects_v3_0_w_RS.Files) AS _Files WHERE (((IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER('Stage I')) OR (IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER('Stage II'))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER('Kidney')))) as results WHERE rn = 1) select (SELECT COUNT(DISTINCT id) from flattened_result) as total, (SELECT COUNT(DISTINCT _Files) from flattened_result) as files, (select ARRAY(select as STRUCT system, count(distinct id) as count from flattened_result group by system)) as system, (select ARRAY(select as STRUCT sex, count(distinct id) as count from flattened_result group by sex)) as sex, (select ARRAY(select as STRUCT race, count(distinct id) as count from flattened_result group by race)) as race, (select ARRAY(select as STRUCT ethnicity, count(distinct id) as count from flattened_result group by ethnicity)) as ethnicity, (select ARRAY(select as STRUCT cause_of_death, count(distinct id) as count from flattened_result group by cause_of_death)) as cause_of_death"),
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            SpecimenCountSqlGenerator.class,
            "with flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY _ResearchSubject_Specimen.id, _ResearchSubject.id, _ResearchSubject_Specimen_identifier.system, _ResearchSubject_Specimen_Files) as rn, _ResearchSubject_Specimen.id AS id, _ResearchSubject_Specimen_identifier.system AS system, _ResearchSubject_Specimen_identifier.value AS value, _ResearchSubject_Specimen.associated_project AS associated_project, _ResearchSubject_Specimen.age_at_collection AS age_at_collection, _ResearchSubject_Specimen.primary_disease_type AS primary_disease_type, _ResearchSubject_Specimen.anatomical_site AS anatomical_site, _ResearchSubject_Specimen.source_material_type AS source_material_type, _ResearchSubject_Specimen.specimen_type AS specimen_type, _ResearchSubject_Specimen.derived_from_specimen AS derived_from_specimen, _ResearchSubject_Specimen.derived_from_subject AS derived_from_subject, _ResearchSubject_Specimen_Files FROM gdc-bq-sample.dev.all_Subjects_v3_0_w_RS AS all_Subjects_v3_0_w_RS INNER JOIN UNNEST(all_Subjects_v3_0_w_RS.ResearchSubject) AS _ResearchSubject INNER JOIN UNNEST(_ResearchSubject.Specimen) AS _ResearchSubject_Specimen LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _ResearchSubject_Diagnosis INNER JOIN UNNEST(_ResearchSubject_Specimen.identifier) AS _ResearchSubject_Specimen_identifier LEFT JOIN UNNEST(_ResearchSubject_Specimen.Files) AS _ResearchSubject_Specimen_Files WHERE (((IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER('Stage I')) OR (IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER('Stage II'))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER('Kidney')))) as results WHERE rn = 1) select (SELECT COUNT(DISTINCT id) from flattened_result) as total, (SELECT COUNT(DISTINCT _ResearchSubject_Specimen_Files) from flattened_result) as files, (select ARRAY(select as STRUCT system, count(distinct id) as count from flattened_result group by system)) as system, (select ARRAY(select as STRUCT primary_disease_type, count(distinct id) as count from flattened_result group by primary_disease_type)) as primary_disease_type, (select ARRAY(select as STRUCT source_material_type, count(distinct id) as count from flattened_result group by source_material_type)) as source_material_type, (select ARRAY(select as STRUCT specimen_type, count(distinct id) as count from flattened_result group by specimen_type)) as specimen_type"),
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            DiagnosisCountSqlGenerator.class,
            "with flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY _ResearchSubject_Diagnosis.id, _ResearchSubject.id, _ResearchSubject_Diagnosis_identifier.system) as rn, _ResearchSubject_Diagnosis.id AS id, _ResearchSubject_Diagnosis_identifier.system AS system, _ResearchSubject_Diagnosis_identifier.value AS value, _ResearchSubject_Diagnosis.primary_diagnosis AS primary_diagnosis, _ResearchSubject_Diagnosis.age_at_diagnosis AS age_at_diagnosis, _ResearchSubject_Diagnosis.morphology AS morphology, _ResearchSubject_Diagnosis.stage AS stage, _ResearchSubject_Diagnosis.grade AS grade, _ResearchSubject_Diagnosis.method_of_diagnosis AS method_of_diagnosis FROM gdc-bq-sample.dev.all_Subjects_v3_0_w_RS AS all_Subjects_v3_0_w_RS INNER JOIN UNNEST(all_Subjects_v3_0_w_RS.ResearchSubject) AS _ResearchSubject INNER JOIN UNNEST(_ResearchSubject.Diagnosis) AS _ResearchSubject_Diagnosis INNER JOIN UNNEST(_ResearchSubject_Diagnosis.identifier) AS _ResearchSubject_Diagnosis_identifier WHERE (((IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER('Stage I')) OR (IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER('Stage II'))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER('Kidney')))) as results WHERE rn = 1) select (SELECT COUNT(DISTINCT id) from flattened_result) as total, (select ARRAY(select as STRUCT system, count(distinct id) as count from flattened_result group by system)) as system, (select ARRAY(select as STRUCT primary_diagnosis, count(distinct id) as count from flattened_result group by primary_diagnosis)) as primary_diagnosis, (select ARRAY(select as STRUCT stage, count(distinct id) as count from flattened_result group by stage)) as stage, (select ARRAY(select as STRUCT grade, count(distinct id) as count from flattened_result group by grade)) as grade"),
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            TreatmentCountSqlGenerator.class,
            "with flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY _ResearchSubject_Diagnosis_Treatment.id, _ResearchSubject.id, _ResearchSubject_Diagnosis.id, _ResearchSubject_Diagnosis_Treatment_identifier.system) as rn, _ResearchSubject_Diagnosis_Treatment.id AS id, _ResearchSubject_Diagnosis_Treatment_identifier.system AS system, _ResearchSubject_Diagnosis_Treatment_identifier.value AS value, _ResearchSubject_Diagnosis_Treatment.treatment_type AS treatment_type, _ResearchSubject_Diagnosis_Treatment.treatment_outcome AS treatment_outcome, _ResearchSubject_Diagnosis_Treatment.days_to_treatment_start AS days_to_treatment_start, _ResearchSubject_Diagnosis_Treatment.days_to_treatment_end AS days_to_treatment_end, _ResearchSubject_Diagnosis_Treatment.therapeutic_agent AS therapeutic_agent, _ResearchSubject_Diagnosis_Treatment.treatment_anatomic_site AS treatment_anatomic_site, _ResearchSubject_Diagnosis_Treatment.treatment_effect AS treatment_effect, _ResearchSubject_Diagnosis_Treatment.treatment_end_reason AS treatment_end_reason, _ResearchSubject_Diagnosis_Treatment.number_of_cycles AS number_of_cycles FROM gdc-bq-sample.dev.all_Subjects_v3_0_w_RS AS all_Subjects_v3_0_w_RS INNER JOIN UNNEST(all_Subjects_v3_0_w_RS.ResearchSubject) AS _ResearchSubject INNER JOIN UNNEST(_ResearchSubject.Diagnosis) AS _ResearchSubject_Diagnosis INNER JOIN UNNEST(_ResearchSubject_Diagnosis.Treatment) AS _ResearchSubject_Diagnosis_Treatment INNER JOIN UNNEST(_ResearchSubject_Diagnosis_Treatment.identifier) AS _ResearchSubject_Diagnosis_Treatment_identifier WHERE (((IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER('Stage I')) OR (IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER('Stage II'))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER('Kidney')))) as results WHERE rn = 1) select (SELECT COUNT(DISTINCT id) from flattened_result) as total, (select ARRAY(select as STRUCT system, count(distinct id) as count from flattened_result group by system)) as system, (select ARRAY(select as STRUCT treatment_type, count(distinct id) as count from flattened_result group by treatment_type)) as treatment_type, (select ARRAY(select as STRUCT treatment_effect, count(distinct id) as count from flattened_result group by treatment_effect)) as treatment_effect"),
        Arguments.of(
            "query-file.json",
            QUALIFIED_TABLE,
            TABLE,
            SubjectCountSqlGenerator.class,
            "with flattened_result as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY all_Subjects_v3_0_w_RS.id, _identifier.system, _subject_associated_project, _Files) as rn, all_Subjects_v3_0_w_RS.id AS id, _identifier.system AS system, _identifier.value AS value, all_Subjects_v3_0_w_RS.species AS species, all_Subjects_v3_0_w_RS.sex AS sex, all_Subjects_v3_0_w_RS.race AS race, all_Subjects_v3_0_w_RS.ethnicity AS ethnicity, all_Subjects_v3_0_w_RS.days_to_birth AS days_to_birth, _subject_associated_project, all_Subjects_v3_0_w_RS.vital_status AS vital_status, all_Subjects_v3_0_w_RS.age_at_death AS age_at_death, all_Subjects_v3_0_w_RS.cause_of_death AS cause_of_death, _Files FROM gdc-bq-sample.dev.all_Subjects_v3_0_w_RS AS all_Subjects_v3_0_w_RS LEFT JOIN UNNEST(all_Subjects_v3_0_w_RS.Files) AS _Files LEFT JOIN gdc-bq-sample.dev.all_Files_v3_0_w_RS AS all_Files_v3_0_w_RS ON all_Files_v3_0_w_RS.id = _Files INNER JOIN UNNEST(all_Subjects_v3_0_w_RS.identifier) AS _identifier INNER JOIN UNNEST(all_Subjects_v3_0_w_RS.subject_associated_project) AS _subject_associated_project WHERE (IFNULL(UPPER(all_Files_v3_0_w_RS.data_modality), '') = UPPER('Genomic'))) as results WHERE rn = 1) select (SELECT COUNT(DISTINCT id) from flattened_result) as total, (SELECT COUNT(DISTINCT _Files) from flattened_result) as files, (select ARRAY(select as STRUCT system, count(distinct id) as count from flattened_result group by system)) as system, (select ARRAY(select as STRUCT sex, count(distinct id) as count from flattened_result group by sex)) as sex, (select ARRAY(select as STRUCT race, count(distinct id) as count from flattened_result group by race)) as race, (select ARRAY(select as STRUCT ethnicity, count(distinct id) as count from flattened_result group by ethnicity)) as ethnicity, (select ARRAY(select as STRUCT cause_of_death, count(distinct id) as count from flattened_result group by cause_of_death)) as cause_of_death"));
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
    String translatedQuery = ct.newInstance(qualifiedTable, query, table).generate();

    assertEquals(expectedSql, translatedQuery);
  }
}
