package bio.terra.cda.app.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class QueryUtilTest {

  @Test
  public void testQuery() throws Exception {
    var unnestDict =
        QueryUtil.makeUnnestDictionary(Path.of("src/test/resources/queryt/schema.json"));

    var w1 =
        new QueryUtil.Where(new QueryUtil.Column("age_at_index"), ">=", new QueryUtil.Value(50));
    var w2 =
        new QueryUtil.Where(
            new QueryUtil.Column("project_id"), "like", new QueryUtil.Value("TCGA%"));
    var w3 =
        new QueryUtil.Where(
            new QueryUtil.Column("figo_stage"), "=", new QueryUtil.Value("Stage IIIC"));

    var w4 = new QueryUtil.Where(w1, "and", w2);
    var w5 = new QueryUtil.Where(w4, "and", w3);

    var s =
        new QueryUtil.Select(
            "case_id", "age_at_index", "gender", "race", "project_id", "figo_stage");

    var q = new QueryUtil.Query("gdc-bq-sample.gdc_metadata.r24_clinical", s, w5);

    assertEquals(
        "SELECT case_id, age_at_index, gender, race, project_id, figo_stage "
            + "FROM gdc-bq-sample.gdc_metadata.r24_clinical, "
            + "unnest(demographic), unnest(project), unnest(diagnoses) "
            + "WHERE (((age_at_index >= 50) and (project_id like 'TCGA%')) and (figo_stage = 'Stage IIIC'))",
        q.translate(unnestDict));
  }
}
