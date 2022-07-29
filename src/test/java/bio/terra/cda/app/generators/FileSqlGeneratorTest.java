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

public class FileSqlGeneratorTest {
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
            "with ResearchSubject_Specimen_files as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY all_Files_v3_0_w_RS.id, _ResearchSubject.id, _ResearchSubject_Specimen.id, all_Subjects_v3_0_w_RS.id) as rn, all_Files_v3_0_w_RS.id AS id, all_Files_v3_0_w_RS.identifier AS identifier, all_Files_v3_0_w_RS.label AS label, all_Files_v3_0_w_RS.data_category AS data_category, all_Files_v3_0_w_RS.data_type AS data_type, all_Files_v3_0_w_RS.file_format AS file_format, all_Files_v3_0_w_RS.associated_project AS associated_project, all_Files_v3_0_w_RS.drs_uri AS drs_uri, all_Files_v3_0_w_RS.byte_size AS byte_size, all_Files_v3_0_w_RS.checksum AS checksum, all_Files_v3_0_w_RS.data_modality AS data_modality, all_Files_v3_0_w_RS.imaging_modality AS imaging_modality, all_Files_v3_0_w_RS.dbgap_accession_number AS dbgap_accession_number, _ResearchSubject_Specimen.id AS researchsubject_specimen_id, _ResearchSubject.id AS researchsubject_id, all_Subjects_v3_0_w_RS.id as subject_id FROM gdc-bq-sample.dev.all_Subjects_v3_0_w_RS AS all_Subjects_v3_0_w_RS INNER JOIN UNNEST(all_Subjects_v3_0_w_RS.ResearchSubject) AS _ResearchSubject INNER JOIN UNNEST(_ResearchSubject.Specimen) AS _ResearchSubject_Specimen INNER JOIN UNNEST(_ResearchSubject_Specimen.Files) AS _ResearchSubject_Specimen_Files INNER JOIN gdc-bq-sample.dev.all_Files_v3_0_w_RS AS all_Files_v3_0_w_RS ON all_Files_v3_0_w_RS.id = _ResearchSubject_Specimen_Files LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _ResearchSubject_Diagnosis WHERE (((IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER(@_ResearchSubject_Diagnosis_stage_1)) OR (IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER(@_ResearchSubject_Diagnosis_stage_2))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@_ResearchSubject_primary_diagnosis_site_1)))) as results WHERE rn = 1), ResearchSubject_files as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY all_Files_v3_0_w_RS.id, _ResearchSubject.id, all_Subjects_v3_0_w_RS.id) as rn, all_Files_v3_0_w_RS.id AS id, all_Files_v3_0_w_RS.identifier AS identifier, all_Files_v3_0_w_RS.label AS label, all_Files_v3_0_w_RS.data_category AS data_category, all_Files_v3_0_w_RS.data_type AS data_type, all_Files_v3_0_w_RS.file_format AS file_format, all_Files_v3_0_w_RS.associated_project AS associated_project, all_Files_v3_0_w_RS.drs_uri AS drs_uri, all_Files_v3_0_w_RS.byte_size AS byte_size, all_Files_v3_0_w_RS.checksum AS checksum, all_Files_v3_0_w_RS.data_modality AS data_modality, all_Files_v3_0_w_RS.imaging_modality AS imaging_modality, all_Files_v3_0_w_RS.dbgap_accession_number AS dbgap_accession_number, '' AS researchsubject_specimen_id, _ResearchSubject.id AS researchsubject_id, all_Subjects_v3_0_w_RS.id as subject_id FROM gdc-bq-sample.dev.all_Subjects_v3_0_w_RS AS all_Subjects_v3_0_w_RS INNER JOIN UNNEST(all_Subjects_v3_0_w_RS.ResearchSubject) AS _ResearchSubject INNER JOIN UNNEST(_ResearchSubject.Files) AS _ResearchSubject_Files INNER JOIN gdc-bq-sample.dev.all_Files_v3_0_w_RS AS all_Files_v3_0_w_RS ON all_Files_v3_0_w_RS.id = _ResearchSubject_Files LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _ResearchSubject_Diagnosis WHERE (((IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER(@_ResearchSubject_Diagnosis_stage_3)) OR (IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER(@_ResearchSubject_Diagnosis_stage_4))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@_ResearchSubject_primary_diagnosis_site_2)))) as results WHERE rn = 1 AND CONCAT(results.id, results.researchsubject_id, results.subject_id) not in (SELECT CONCAT(ResearchSubject_Specimen_files.id, ResearchSubject_Specimen_files.researchsubject_id, ResearchSubject_Specimen_files.subject_id) FROM ResearchSubject_Specimen_files)), Subject_files as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY all_Files_v3_0_w_RS.id, all_Subjects_v3_0_w_RS.id) as rn, all_Files_v3_0_w_RS.id AS id, all_Files_v3_0_w_RS.identifier AS identifier, all_Files_v3_0_w_RS.label AS label, all_Files_v3_0_w_RS.data_category AS data_category, all_Files_v3_0_w_RS.data_type AS data_type, all_Files_v3_0_w_RS.file_format AS file_format, all_Files_v3_0_w_RS.associated_project AS associated_project, all_Files_v3_0_w_RS.drs_uri AS drs_uri, all_Files_v3_0_w_RS.byte_size AS byte_size, all_Files_v3_0_w_RS.checksum AS checksum, all_Files_v3_0_w_RS.data_modality AS data_modality, all_Files_v3_0_w_RS.imaging_modality AS imaging_modality, all_Files_v3_0_w_RS.dbgap_accession_number AS dbgap_accession_number, '' AS researchsubject_specimen_id, '' AS researchsubject_id, all_Subjects_v3_0_w_RS.id as subject_id FROM gdc-bq-sample.dev.all_Subjects_v3_0_w_RS AS all_Subjects_v3_0_w_RS INNER JOIN UNNEST(all_Subjects_v3_0_w_RS.Files) AS _Files INNER JOIN gdc-bq-sample.dev.all_Files_v3_0_w_RS AS all_Files_v3_0_w_RS ON all_Files_v3_0_w_RS.id = _Files LEFT JOIN UNNEST(all_Subjects_v3_0_w_RS.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _ResearchSubject_Diagnosis WHERE (((IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER(@_ResearchSubject_Diagnosis_stage_5)) OR (IFNULL(UPPER(_ResearchSubject_Diagnosis.stage), '') = UPPER(@_ResearchSubject_Diagnosis_stage_6))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@_ResearchSubject_primary_diagnosis_site_3)))) as results WHERE rn = 1 AND CONCAT(results.id, results.subject_id) not in (SELECT CONCAT(ResearchSubject_files.id, ResearchSubject_files.subject_id) FROM ResearchSubject_files)),unioned_result as (SELECT ResearchSubject_Specimen_files.* FROM ResearchSubject_Specimen_files UNION ALL SELECT ResearchSubject_files.* FROM ResearchSubject_files UNION ALL SELECT Subject_files.* FROM Subject_files) SELECT unioned_result.* FROM unioned_result"));
  }

  @ParameterizedTest
  @MethodSource("queryData")
  void testQuery(String queryFile, String qualifiedTable, String table, String expectedQueryFormat)
      throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve(queryFile));
    String expectedSql = String.format(expectedQueryFormat, qualifiedTable, table);

    Query query = objectMapper.readValue(jsonQuery, Query.class);

    String translatedQuery =
        new FileSqlGenerator(qualifiedTable, query, table).generate().build().getQuery();

    assertEquals(expectedSql, translatedQuery);
  }
}
