package bio.terra.cda.app.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

class QueryTranslatorTest {

  static final String jsonQueryBasePath = "src/test/java/bio/terra/cda/app/util/";

  @Test
  public void testQuerySimple() throws Exception {
    String table = "gdc-bq-sample.gdc_metadata.r26_clinical";
    String jsonQueryPath = jsonQueryBasePath + "query1.json";
    String jsonQuery = Files.readString(Paths.get(jsonQueryPath), StandardCharsets.US_ASCII);

    String EXPECTED_SQL =
        String.format(
            "SELECT * FROM %s, UNNEST(project) AS _project WHERE (_project.project_id = 'TCGA-OV')",
            table);

    Query query = new ObjectMapper().readValue(jsonQuery, Query.class);
    String translatedQuery = (new QueryTranslator(table, query)).sql();

    assertEquals(EXPECTED_SQL, translatedQuery);
  }

  @Test
  public void testQueryComplex() throws Exception {
    String table = "gdc-bq-sample.gdc_metadata.r26_clinical";
    String jsonQueryPath = jsonQueryBasePath + "query2.json";
    String jsonQuery = Files.readString(Paths.get(jsonQueryPath), StandardCharsets.US_ASCII);

    String EXPECTED_SQL =
        String.format(
            "SELECT * FROM %s, UNNEST(demographic) AS _demographic, UNNEST(project) AS _project, UNNEST(diagnoses) AS _diagnoses WHERE (((_demographic.age_at_index >= 50) AND (_project.project_id = 'TCGA-OV')) AND (_diagnoses.figo_stage = 'Stage IIIC'))",
            table);

    Query query = new ObjectMapper().readValue(jsonQuery, Query.class);
    String translatedQuery = (new QueryTranslator(table, query)).sql();

    assertEquals(EXPECTED_SQL, translatedQuery);
  }
}
