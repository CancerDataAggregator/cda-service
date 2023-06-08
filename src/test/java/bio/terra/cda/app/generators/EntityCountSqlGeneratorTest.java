package bio.terra.cda.app.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;

import bio.terra.cda.app.operators.QueryModule;
import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class EntityCountSqlGeneratorTest {

  static final Path TEST_FILES = Paths.get("src/test/resources/query");

  public static final String TABLE = "subject";

  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new QueryModule());

  public static Stream<Arguments> queryData() {
    return Stream.of(
        Arguments.of(
            "query-kidney.json",
            TABLE,
            TABLE,
            ResearchSubjectCountSqlGenerator.class,
            "WITH flattened_result as (SELECT researchsubject.id AS researchsubject_id, file_subject.file_id AS file_id, researchsubject_identifier.system AS researchsubject_identifier_system, researchsubject.primary_diagnosis_condition AS primary_diagnosis_condition, researchsubject.primary_diagnosis_site AS primary_diagnosis_site FROM researchsubject AS researchsubject  LEFT JOIN researchsubject_diagnosis AS researchsubject_diagnosis ON researchsubject.id = researchsubject_diagnosis.researchsubject_id  LEFT JOIN diagnosis AS diagnosis ON researchsubject_diagnosis.diagnosis_id = diagnosis.id  INNER JOIN subject_researchsubject AS subject_researchsubject ON researchsubject.id = subject_researchsubject.researchsubject_id  LEFT JOIN subject AS subject ON subject_researchsubject.subject_id = subject.id  LEFT JOIN file_subject AS file_subject ON subject.id = file_subject.subject_id  INNER JOIN researchsubject_identifier AS researchsubject_identifier ON researchsubject.id = researchsubject_identifier.researchsubject_id WHERE (((COALESCE(UPPER(stage), '') = UPPER('IIA')) OR (COALESCE(UPPER(stage), '') = UPPER('IIB'))) AND (COALESCE(UPPER(primary_diagnosis_site), '') = UPPER('Lung')))), researchsubject_identifier_system_count as (SELECT row_to_json(subq) AS json_researchsubject_identifier_system FROM (select researchsubject_identifier_system as researchsubject_identifier_system, count(distinct researchsubject_id) as count from flattened_result group by researchsubject_identifier_system) as subq), primary_diagnosis_condition_count as (SELECT row_to_json(subq) AS json_primary_diagnosis_condition FROM (select primary_diagnosis_condition as primary_diagnosis_condition, count(distinct researchsubject_id) as count from flattened_result group by primary_diagnosis_condition) as subq), primary_diagnosis_site_count as (SELECT row_to_json(subq) AS json_primary_diagnosis_site FROM (select primary_diagnosis_site as primary_diagnosis_site, count(distinct researchsubject_id) as count from flattened_result group by primary_diagnosis_site) as subq)  select (SELECT COUNT(DISTINCT researchsubject_id) from flattened_result) as researchsubject_id, (SELECT COUNT(DISTINCT file_id) from flattened_result) as file_id, (SELECT array_agg(json_researchsubject_identifier_system) from researchsubject_identifier_system_count) as researchsubject_identifier_system, (SELECT array_agg(json_primary_diagnosis_condition) from primary_diagnosis_condition_count) as primary_diagnosis_condition, (SELECT array_agg(json_primary_diagnosis_site) from primary_diagnosis_site_count) as primary_diagnosis_site"),
        Arguments.of(
            "query-kidney.json",
            TABLE,
            TABLE,
            SubjectCountSqlGenerator.class,
            "WITH flattened_result as (SELECT subject.id AS subject_id, file_subject.file_id AS file_id, subject_identifier.system AS subject_identifier_system, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.cause_of_death AS cause_of_death FROM subject AS subject  LEFT JOIN subject_researchsubject AS subject_researchsubject ON subject.id = subject_researchsubject.subject_id  LEFT JOIN researchsubject AS researchsubject ON subject_researchsubject.researchsubject_id = researchsubject.id  LEFT JOIN researchsubject_diagnosis AS researchsubject_diagnosis ON researchsubject.id = researchsubject_diagnosis.researchsubject_id  LEFT JOIN diagnosis AS diagnosis ON researchsubject_diagnosis.diagnosis_id = diagnosis.id  INNER JOIN file_subject AS file_subject ON subject.id = file_subject.subject_id  INNER JOIN subject_identifier AS subject_identifier ON subject.id = subject_identifier.subject_id WHERE (((COALESCE(UPPER(stage), '') = UPPER('IIA')) OR (COALESCE(UPPER(stage), '') = UPPER('IIB'))) AND (COALESCE(UPPER(primary_diagnosis_site), '') = UPPER('Lung')))), subject_identifier_system_count as (SELECT row_to_json(subq) AS json_subject_identifier_system FROM (select subject_identifier_system as subject_identifier_system, count(distinct subject_id) as count from flattened_result group by subject_identifier_system) as subq), sex_count as (SELECT row_to_json(subq) AS json_sex FROM (select sex as sex, count(distinct subject_id) as count from flattened_result group by sex) as subq), race_count as (SELECT row_to_json(subq) AS json_race FROM (select race as race, count(distinct subject_id) as count from flattened_result group by race) as subq), ethnicity_count as (SELECT row_to_json(subq) AS json_ethnicity FROM (select ethnicity as ethnicity, count(distinct subject_id) as count from flattened_result group by ethnicity) as subq), cause_of_death_count as (SELECT row_to_json(subq) AS json_cause_of_death FROM (select cause_of_death as cause_of_death, count(distinct subject_id) as count from flattened_result group by cause_of_death) as subq)  select (SELECT COUNT(DISTINCT subject_id) from flattened_result) as subject_id, (SELECT COUNT(DISTINCT file_id) from flattened_result) as file_id, (SELECT array_agg(json_subject_identifier_system) from subject_identifier_system_count) as subject_identifier_system, (SELECT array_agg(json_sex) from sex_count) as sex, (SELECT array_agg(json_race) from race_count) as race, (SELECT array_agg(json_ethnicity) from ethnicity_count) as ethnicity, (SELECT array_agg(json_cause_of_death) from cause_of_death_count) as cause_of_death"),
        Arguments.of(
            "query-kidney.json",
            TABLE,
            TABLE,
            SpecimenCountSqlGenerator.class,
            "WITH flattened_result as (SELECT specimen.id AS specimen_id, file_specimen.file_id AS file_id, specimen_identifier.system AS specimen_identifier_system, specimen.primary_disease_type AS primary_disease_type, specimen.source_material_type AS source_material_type, specimen.specimen_type AS specimen_type FROM specimen AS specimen  LEFT JOIN researchsubject_specimen AS researchsubject_specimen ON specimen.id = researchsubject_specimen.specimen_id  LEFT JOIN researchsubject AS researchsubject ON researchsubject_specimen.researchsubject_id = researchsubject.id  LEFT JOIN researchsubject_diagnosis AS researchsubject_diagnosis ON researchsubject.id = researchsubject_diagnosis.researchsubject_id  LEFT JOIN diagnosis AS diagnosis ON researchsubject_diagnosis.diagnosis_id = diagnosis.id  INNER JOIN file_specimen AS file_specimen ON specimen.id = file_specimen.specimen_id  INNER JOIN specimen_identifier AS specimen_identifier ON specimen.id = specimen_identifier.specimen_id WHERE (((COALESCE(UPPER(stage), '') = UPPER('IIA')) OR (COALESCE(UPPER(stage), '') = UPPER('IIB'))) AND (COALESCE(UPPER(primary_diagnosis_site), '') = UPPER('Lung')))), specimen_identifier_system_count as (SELECT row_to_json(subq) AS json_specimen_identifier_system FROM (select specimen_identifier_system as specimen_identifier_system, count(distinct specimen_id) as count from flattened_result group by specimen_identifier_system) as subq), primary_disease_type_count as (SELECT row_to_json(subq) AS json_primary_disease_type FROM (select primary_disease_type as primary_disease_type, count(distinct specimen_id) as count from flattened_result group by primary_disease_type) as subq), source_material_type_count as (SELECT row_to_json(subq) AS json_source_material_type FROM (select source_material_type as source_material_type, count(distinct specimen_id) as count from flattened_result group by source_material_type) as subq), specimen_type_count as (SELECT row_to_json(subq) AS json_specimen_type FROM (select specimen_type as specimen_type, count(distinct specimen_id) as count from flattened_result group by specimen_type) as subq)  select (SELECT COUNT(DISTINCT specimen_id) from flattened_result) as specimen_id, (SELECT COUNT(DISTINCT file_id) from flattened_result) as file_id, (SELECT array_agg(json_specimen_identifier_system) from specimen_identifier_system_count) as specimen_identifier_system, (SELECT array_agg(json_primary_disease_type) from primary_disease_type_count) as primary_disease_type, (SELECT array_agg(json_source_material_type) from source_material_type_count) as source_material_type, (SELECT array_agg(json_specimen_type) from specimen_type_count) as specimen_type"),
        Arguments.of(
            "query-kidney.json",
            TABLE,
            TABLE,
            DiagnosisCountSqlGenerator.class,
            "WITH flattened_result as (SELECT diagnosis.id AS diagnosis_id, diagnosis_identifier.system AS diagnosis_identifier_system, diagnosis.primary_diagnosis AS primary_diagnosis, diagnosis.stage AS stage, diagnosis.grade AS grade FROM diagnosis AS diagnosis  LEFT JOIN researchsubject_diagnosis AS researchsubject_diagnosis ON diagnosis.id = researchsubject_diagnosis.diagnosis_id  LEFT JOIN researchsubject AS researchsubject ON researchsubject_diagnosis.researchsubject_id = researchsubject.id  INNER JOIN diagnosis_identifier AS diagnosis_identifier ON diagnosis.id = diagnosis_identifier.diagnosis_id WHERE (((COALESCE(UPPER(stage), '') = UPPER('IIA')) OR (COALESCE(UPPER(stage), '') = UPPER('IIB'))) AND (COALESCE(UPPER(primary_diagnosis_site), '') = UPPER('Lung')))), diagnosis_identifier_system_count as (SELECT row_to_json(subq) AS json_diagnosis_identifier_system FROM (select diagnosis_identifier_system as diagnosis_identifier_system, count(distinct diagnosis_id) as count from flattened_result group by diagnosis_identifier_system) as subq), primary_diagnosis_count as (SELECT row_to_json(subq) AS json_primary_diagnosis FROM (select primary_diagnosis as primary_diagnosis, count(distinct diagnosis_id) as count from flattened_result group by primary_diagnosis) as subq), stage_count as (SELECT row_to_json(subq) AS json_stage FROM (select stage as stage, count(distinct diagnosis_id) as count from flattened_result group by stage) as subq), grade_count as (SELECT row_to_json(subq) AS json_grade FROM (select grade as grade, count(distinct diagnosis_id) as count from flattened_result group by grade) as subq)  select (SELECT COUNT(DISTINCT diagnosis_id) from flattened_result) as diagnosis_id, (SELECT array_agg(json_diagnosis_identifier_system) from diagnosis_identifier_system_count) as diagnosis_identifier_system, (SELECT array_agg(json_primary_diagnosis) from primary_diagnosis_count) as primary_diagnosis, (SELECT array_agg(json_stage) from stage_count) as stage, (SELECT array_agg(json_grade) from grade_count) as grade"),
        Arguments.of(
            "query-kidney.json",
            TABLE,
            TABLE,
            TreatmentCountSqlGenerator.class,
            "WITH flattened_result as (SELECT treatment.id AS treatment_id, treatment_identifier.system AS treatment_identifier_system, treatment.treatment_type AS treatment_type, treatment.treatment_effect AS treatment_effect FROM treatment AS treatment  LEFT JOIN diagnosis_treatment AS diagnosis_treatment ON treatment.id = diagnosis_treatment.treatment_id  LEFT JOIN diagnosis AS diagnosis ON diagnosis_treatment.diagnosis_id = diagnosis.id  LEFT JOIN researchsubject_treatment AS researchsubject_treatment ON treatment.id = researchsubject_treatment.treatment_id  LEFT JOIN researchsubject AS researchsubject ON researchsubject_treatment.researchsubject_id = researchsubject.id  INNER JOIN treatment_identifier AS treatment_identifier ON treatment.id = treatment_identifier.treatment_id WHERE (((COALESCE(UPPER(stage), '') = UPPER('IIA')) OR (COALESCE(UPPER(stage), '') = UPPER('IIB'))) AND (COALESCE(UPPER(primary_diagnosis_site), '') = UPPER('Lung')))), treatment_identifier_system_count as (SELECT row_to_json(subq) AS json_treatment_identifier_system FROM (select treatment_identifier_system as treatment_identifier_system, count(distinct treatment_id) as count from flattened_result group by treatment_identifier_system) as subq), treatment_type_count as (SELECT row_to_json(subq) AS json_treatment_type FROM (select treatment_type as treatment_type, count(distinct treatment_id) as count from flattened_result group by treatment_type) as subq), treatment_effect_count as (SELECT row_to_json(subq) AS json_treatment_effect FROM (select treatment_effect as treatment_effect, count(distinct treatment_id) as count from flattened_result group by treatment_effect) as subq)  select (SELECT COUNT(DISTINCT treatment_id) from flattened_result) as treatment_id, (SELECT array_agg(json_treatment_identifier_system) from treatment_identifier_system_count) as treatment_identifier_system, (SELECT array_agg(json_treatment_type) from treatment_type_count) as treatment_type, (SELECT array_agg(json_treatment_effect) from treatment_effect_count) as treatment_effect"),
        Arguments.of(
            "query-file.json",
            TABLE,
            TABLE,
            SubjectCountSqlGenerator.class,
            "WITH flattened_result as (SELECT subject.id AS subject_id, file_subject.file_id AS file_id, subject_identifier.system AS subject_identifier_system, subject.sex AS sex, subject.race AS race, subject.ethnicity AS ethnicity, subject.cause_of_death AS cause_of_death FROM subject AS subject  INNER JOIN file_subject AS file_subject ON subject.id = file_subject.subject_id  LEFT JOIN file AS file ON file_subject.file_id = file.id  INNER JOIN subject_identifier AS subject_identifier ON subject.id = subject_identifier.subject_id WHERE (COALESCE(UPPER(data_modality), '') = UPPER('Genomic'))), subject_identifier_system_count as (SELECT row_to_json(subq) AS json_subject_identifier_system FROM (select subject_identifier_system as subject_identifier_system, count(distinct subject_id) as count from flattened_result group by subject_identifier_system) as subq), sex_count as (SELECT row_to_json(subq) AS json_sex FROM (select sex as sex, count(distinct subject_id) as count from flattened_result group by sex) as subq), race_count as (SELECT row_to_json(subq) AS json_race FROM (select race as race, count(distinct subject_id) as count from flattened_result group by race) as subq), ethnicity_count as (SELECT row_to_json(subq) AS json_ethnicity FROM (select ethnicity as ethnicity, count(distinct subject_id) as count from flattened_result group by ethnicity) as subq), cause_of_death_count as (SELECT row_to_json(subq) AS json_cause_of_death FROM (select cause_of_death as cause_of_death, count(distinct subject_id) as count from flattened_result group by cause_of_death) as subq)  select (SELECT COUNT(DISTINCT subject_id) from flattened_result) as subject_id, (SELECT COUNT(DISTINCT file_id) from flattened_result) as file_id, (SELECT array_agg(json_subject_identifier_system) from subject_identifier_system_count) as subject_identifier_system, (SELECT array_agg(json_sex) from sex_count) as sex, (SELECT array_agg(json_race) from race_count) as race, (SELECT array_agg(json_ethnicity) from ethnicity_count) as ethnicity, (SELECT array_agg(json_cause_of_death) from cause_of_death_count) as cause_of_death")
    );
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
        clazz.getDeclaredConstructor(Query.class);
    String translatedQuery =
        ct.newInstance(query).getReadableQuerySql();

    assertEquals(expectedSql, translatedQuery);
  }
}
