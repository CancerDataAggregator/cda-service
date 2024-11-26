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

public class  FileSqlGeneratorTest {
  static final Path TEST_FILES = Paths.get("src/test/resources/query");

  public static final String TABLE = "subjects";
  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new QueryModule());

  public static Stream<Arguments> queryData() {
    return Stream.of(
        Arguments.of(
            "query-test-lung.json",
            TABLE,
            TABLE,
            "SELECT file.id AS file_id, file.label AS label, file.data_category AS data_category, file.data_type AS data_type, file.file_format AS file_format, file.drs_uri AS drs_uri, file.byte_size AS byte_size, file.checksum AS checksum, file.data_modality AS data_modality, file.imaging_modality AS imaging_modality, file.dbgap_accession_number AS dbgap_accession_number, file.imaging_series AS imaging_series, json_agg(distinct (file_identifier.system, file_identifier.field_name, file_identifier.value)::system_data) as file_identifier, json_agg(distinct file_associated_project.associated_project) AS file_associated_project FROM file AS file  LEFT JOIN file_subject AS file_subject ON file.integer_id_alias = file_subject.file_alias  LEFT JOIN subject AS subject ON file_subject.subject_alias = subject.integer_id_alias  LEFT JOIN subject_researchsubject AS subject_researchsubject ON subject.integer_id_alias = subject_researchsubject.subject_alias  LEFT JOIN researchsubject AS researchsubject ON subject_researchsubject.researchsubject_alias = researchsubject.integer_id_alias  LEFT JOIN researchsubject_diagnosis AS researchsubject_diagnosis ON researchsubject.integer_id_alias = researchsubject_diagnosis.researchsubject_alias  LEFT JOIN diagnosis AS diagnosis ON researchsubject_diagnosis.diagnosis_alias = diagnosis.integer_id_alias  INNER JOIN file_associated_project AS file_associated_project ON file.integer_id_alias = file_associated_project.file_alias  INNER JOIN file_identifier AS file_identifier ON file.integer_id_alias = file_identifier.file_alias WHERE (((COALESCE(UPPER(diagnosis.stage), '') = UPPER('IIA')) OR (COALESCE(UPPER(diagnosis.stage), '') = UPPER('IIB'))) AND (COALESCE(UPPER(researchsubject.primary_diagnosis_site), '') = UPPER('Lung'))) GROUP BY file.id,file.label,file.data_category,file.data_type,file.file_format,file.drs_uri,file.byte_size,file.checksum,file.data_modality,file.imaging_modality,file.dbgap_accession_number,file.imaging_series ORDER BY file.id asc"));

  }

  @ParameterizedTest
  @MethodSource("queryData")
  void testQuery(String queryFile, String qualifiedTable, String table, String expectedQueryFormat)
      throws Exception {
//    String jsonQuery = Files.readString(TEST_FILES.resolve(queryFile));
//    String expectedSql = String.format(expectedQueryFormat, qualifiedTable, table);
//
//    Query query = objectMapper.readValue(jsonQuery, Query.class);
//
//    String translatedQuery = new FileSqlGenerator(query).getReadableQuerySql();
//
//    assertEquals(expectedSql, translatedQuery);
  }
}
