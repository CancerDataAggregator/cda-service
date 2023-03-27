package bio.terra.cda.app.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import bio.terra.cda.app.helpers.StorageServiceHelper;
import bio.terra.cda.app.helpers.TableSchemaHelper;
import bio.terra.cda.app.operators.QueryModule;
import bio.terra.cda.app.service.StorageService;
import bio.terra.cda.app.util.TableSchema;
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

  public static final String TABLE = "all_Subjects_v3_0_final";
  public static final String QUALIFIED_TABLE = "gdc-bq-sample.dev." + TABLE;

  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new QueryModule());

  public static Stream<Arguments> queryData() {
    return Stream.of(
        Arguments.of(
            "query-kidney.json",
            QUALIFIED_TABLE,
            TABLE,
            "WITH specimen_files as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY File.id, Subject.id, _ResearchSubject.id, _Specimen.id) as rn, File.id AS file_id, File.identifier AS file_identifier, File.label AS label, File.data_category AS data_category, File.data_type AS data_type, File.file_format AS file_format, File.associated_project AS file_associated_project, File.drs_uri AS drs_uri, File.byte_size AS byte_size, File.checksum AS checksum, File.data_modality AS data_modality, File.imaging_modality AS imaging_modality, File.dbgap_accession_number AS dbgap_accession_number, File.imaging_series AS imaging_series, File.Subject AS null, File.ResearchSubject AS null, File.Specimen AS null, _Specimen.id AS specimen_id, _ResearchSubject.id AS researchsubject_id, Subject.id AS subject_id FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject INNER JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject INNER JOIN UNNEST(_ResearchSubject.Specimen) AS _Specimen LEFT JOIN UNNEST(_Specimen.Files) AS _specimen_Files INNER JOIN gdc-bq-sample.dev.all_Files_v3_0_final AS File ON _specimen_Files = File.id LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis WHERE (((IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_1)) OR (IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_2))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@primary_diagnosis_site_1)))) as results WHERE rn = 1),researchsubject_files as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY File.id, Subject.id, _ResearchSubject.id) as rn, File.id AS file_id, File.identifier AS file_identifier, File.label AS label, File.data_category AS data_category, File.data_type AS data_type, File.file_format AS file_format, File.associated_project AS file_associated_project, File.drs_uri AS drs_uri, File.byte_size AS byte_size, File.checksum AS checksum, File.data_modality AS data_modality, File.imaging_modality AS imaging_modality, File.dbgap_accession_number AS dbgap_accession_number, File.imaging_series AS imaging_series, File.Subject AS null, File.ResearchSubject AS null, File.Specimen AS null, '' AS specimen_id, _ResearchSubject.id AS researchsubject_id, Subject.id AS subject_id FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject INNER JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.Files) AS _researchsubject_Files INNER JOIN gdc-bq-sample.dev.all_Files_v3_0_final AS File ON _researchsubject_Files = File.id LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis WHERE (((IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_3)) OR (IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_4))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@primary_diagnosis_site_2)))) as results WHERE rn = 1 AND CONCAT(results.file_id, results.subject_id, results.researchsubject_id) not in (SELECT CONCAT(specimen_files.file_id, specimen_files.subject_id, specimen_files.researchsubject_id) FROM specimen_files)),subject_files as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY File.id, Subject.id) as rn, File.id AS file_id, File.identifier AS file_identifier, File.label AS label, File.data_category AS data_category, File.data_type AS data_type, File.file_format AS file_format, File.associated_project AS file_associated_project, File.drs_uri AS drs_uri, File.byte_size AS byte_size, File.checksum AS checksum, File.data_modality AS data_modality, File.imaging_modality AS imaging_modality, File.dbgap_accession_number AS dbgap_accession_number, File.imaging_series AS imaging_series, File.Subject AS null, File.ResearchSubject AS null, File.Specimen AS null, '' AS specimen_id, '' AS researchsubject_id, Subject.id AS subject_id FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject LEFT JOIN UNNEST(Subject.Files) AS _subject_Files INNER JOIN gdc-bq-sample.dev.all_Files_v3_0_final AS File ON _subject_Files = File.id LEFT JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis WHERE (((IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_5)) OR (IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@stage_6))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@primary_diagnosis_site_3)))) as results WHERE rn = 1 AND CONCAT(results.file_id, results.subject_id) not in (SELECT CONCAT(researchsubject_files.file_id, researchsubject_files.subject_id) FROM researchsubject_files)),unioned_result as (SELECT specimen_files.* FROM specimen_files UNION ALL SELECT researchsubject_files.* FROM researchsubject_files UNION ALL SELECT subject_files.* FROM subject_files) SELECT unioned_result.* FROM unioned_result"));
  }

  @ParameterizedTest
  @MethodSource("queryData")
  void testQuery(String queryFile, String qualifiedTable, String table, String expectedQueryFormat)
      throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve(queryFile));
    String expectedSql = String.format(expectedQueryFormat, qualifiedTable, table);

    Query query = objectMapper.readValue(jsonQuery, Query.class);
    TableSchema tableSchema = TableSchemaHelper.getNewTableSchema("v3");

    String translatedQuery =
        new FileSqlGenerator(tableSchema, qualifiedTable, query, table).generate().build().getQuery();

    assertEquals(expectedSql, translatedQuery);
  }
}
