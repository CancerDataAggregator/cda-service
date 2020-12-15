package bio.terra.cda.app.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class QueryUtilTest {

  static final String EXPECTED_SQL =
      "SELECT * FROM gdc-bq-sample.gdc_metadata.r26_clinical, UNNEST(demographic) AS _demographic, "
          + "UNNEST(project) AS _project, UNNEST(diagnoses) AS _diagnoses "
          + "WHERE (((_demographic.age_at_index >= 50) AND (_project.project_id = 'TCGA-OV')) AND (_diagnoses.figo_stage = 'Stage IIIC'))";

  @Test
  public void testQuery() throws Exception {
    var c1 = new QueryUtil.Condition("demographic.age_at_index", ">=", 50);
    var c2 = new QueryUtil.Condition("project.project_id", "=", "TCGA-OV");
    var c3 = new QueryUtil.Condition("diagnoses.figo_stage", "=", "Stage IIIC");

    var c = c1.And(c2).And(c3);

    var dataset = new QueryUtil.Dataset("gdc-bq-sample.gdc_metadata.r26_clinical", c);
    assertEquals(EXPECTED_SQL, dataset.sql());
  }
}
