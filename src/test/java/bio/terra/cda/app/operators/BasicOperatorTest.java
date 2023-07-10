package bio.terra.cda.app.operators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.cda.app.generators.SqlGenerator;
import bio.terra.cda.app.helpers.QueryFileReader;
import bio.terra.cda.app.models.RdbmsSchema;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class BasicOperatorTest {
  @Test
  void testInvalidColumn() throws IOException {
    Query query = QueryFileReader.getQueryFromFile("query-invalid-column.json");

    SqlGenerator sqlgen = new SqlGenerator(query, false);
    TableInfo subjectTableInfo = RdbmsSchema.getDataSetInfo().getTableInfo("subject");
    QueryContext ctx = sqlgen.buildQueryContext(subjectTableInfo, false, false);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> ((BasicOperator) query.getWhere()).buildQuery(ctx),
            "Expected query to throw IllegalArgumentException but didn't");
  }

  @Test
  void testEqualsQuoted() throws IOException {
    Query query = QueryFileReader.getQueryFromFile("query-equals-quoted.json");

    SqlGenerator sqlgen = new SqlGenerator(query, false);
    TableInfo subjectTableInfo = RdbmsSchema.getDataSetInfo().getTableInfo("subject");
    String whereClause =
        ((BasicOperator) query.getWhere())
            .buildQuery(sqlgen.buildQueryContext(subjectTableInfo, false, false));

    assertEquals("(COALESCE(UPPER(id), '') = UPPER(:parameter_1))", whereClause);
  }

  @Test
  void testAndOr() throws IOException {
    Query query = QueryFileReader.getQueryFromFile("query-kidney.json");
    SqlGenerator sqlgen = new SqlGenerator(query, false);
    TableInfo subjectTableInfo = RdbmsSchema.getDataSetInfo().getTableInfo("subject");
    String whereClause =
        ((BasicOperator) query.getWhere())
            .buildQuery(sqlgen.buildQueryContext(subjectTableInfo, false, false));

    assertEquals(
        "(((COALESCE(UPPER(stage), '') = UPPER(:parameter_1)) OR (COALESCE(UPPER(stage), '') = UPPER(:parameter_2))) AND (COALESCE(UPPER(primary_diagnosis_site), '') = UPPER(:parameter_3)))",
        whereClause);

    //    QueryContext ResearchSubjectContext = new QueryContext("researchsubject");
    //
    //    String rsWhere = query.buildQuery(ResearchSubjectContext);

    //    assertEquals(1, ResearchSubjectContext.getUnnests().size());

    //    ResearchSubjectContext.getUnnests()
    //        .forEach(
    //            unnest -> {
    //              if (unnest.getPath().equals("Subject.ResearchSubject")) {
    //                assertEquals(SqlUtil.JoinType.INNER, unnest.getJoinType());
    //              } else {
    //                assertEquals(SqlUtil.JoinType.LEFT, unnest.getJoinType());
    //                assertEquals("_ResearchSubject.Diagnosis", unnest.getPath());
    //              }
    //            });
  }
}
