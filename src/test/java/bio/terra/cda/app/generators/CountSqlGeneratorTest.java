package bio.terra.cda.app.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;

import bio.terra.cda.app.operators.QueryModule;
import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.QueryJobConfiguration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CountSqlGeneratorTest {
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
                "WITH flattened_results as (SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY Subject.id, _Specimen.id, _Treatment.id, _Diagnosis.id, Mutation.case_barcode, _ResearchSubject.id) as rn, _Specimen.id AS specimen_id, _Treatment.id AS treatment_id, _Diagnosis.id AS diagnosis_id, Mutation.case_barcode AS case_barcode, _ResearchSubject.id AS researchsubject_id, Subject.id AS subject_id FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject LEFT JOIN UNNEST(Subject.ResearchSubject) AS _ResearchSubject LEFT JOIN UNNEST(_ResearchSubject.Specimen) AS _Specimen LEFT JOIN UNNEST(_ResearchSubject.Diagnosis) AS _Diagnosis LEFT JOIN UNNEST(_Diagnosis.Treatment) AS _Treatment LEFT JOIN gdc-bq-sample.dev.somatic_mutation_hg38_gdc_current AS Mutation ON Subject.id = Mutation.case_barcode WHERE (((IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@parameter_1)) OR (IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@parameter_2))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@parameter_3)))) as results WHERE rn = 1 ) SELECT COUNT(DISTINCT specimen_id) AS specimen_count, COUNT(DISTINCT treatment_id) AS treatment_count, COUNT(DISTINCT diagnosis_id) AS diagnosis_count, COUNT(DISTINCT case_barcode) AS mutation_count, COUNT(DISTINCT researchsubject_id) AS researchsubject_count, COUNT(DISTINCT subject_id) AS subject_count FROM flattened_results"));
  }

  @ParameterizedTest
  @MethodSource("queryData")
  void testQuery(String queryFile, String qualifiedTable, String table, String expectedQueryFormat)
      throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve(queryFile));
    String expectedSql = String.format(expectedQueryFormat, qualifiedTable, table);

    Query query = objectMapper.readValue(jsonQuery, Query.class);

    QueryJobConfiguration config =
        new CountsSqlGenerator(qualifiedTable, query, table).generate().build();
    assertEquals(expectedSql, config.getQuery());
  }
}
