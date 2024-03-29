package bio.terra.cda.app.operators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.cda.app.helpers.QueryFileReader;
import bio.terra.cda.app.helpers.QueryHelper;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlUtil;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class BasicOperatorTest {
  @Test
  void testInvalidColumn() throws IOException {
    BasicOperator query =
        (BasicOperator) QueryFileReader.getQueryFromFile("query-invalid-column.json");

    QueryContext ctx =
        QueryHelper.getNewQueryContext(
            "all_Subjects_v3_0_final", "all_Files_v3_0_final", "Subject", "project", true);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> query.buildQuery(ctx),
            "Expected query to throw IllegalArgumentException but didn't");
  }

  @Test
  void testEqualsQuoted() throws IOException {
    BasicOperator query =
        (BasicOperator) QueryFileReader.getQueryFromFile("query-equals-quoted.json");

    QueryContext ctx =
        QueryHelper.getNewQueryContext(
            "all_Subjects_v3_0_final", "all_Files_v3_0_final", "Subject", "project", true);

    String whereClause = query.buildQuery(ctx);

    assertEquals(0, ctx.getUnnests().size());
    assertEquals(0, ctx.getPartitions().size());
    assertEquals("(IFNULL(UPPER(Subject.id), '') = UPPER(@parameter_1))", whereClause);
  }

  @Test
  void testAndOr() throws IOException {
    BasicOperator query = (BasicOperator) QueryFileReader.getQueryFromFile("query-kidney.json");

    QueryContext ctx =
        QueryHelper.getNewQueryContext(
            "all_Subjects_v3_0_final", "all_Files_v3_0_final", "Subject", "project", true);

    String whereClause = query.buildQuery(ctx);

    assertEquals(2, ctx.getUnnests().size());
    assertEquals(0, ctx.getPartitions().size());
    assertEquals(
        "(((IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@parameter_1)) OR (IFNULL(UPPER(_Diagnosis.stage), '') = UPPER(@parameter_2))) AND (IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') = UPPER(@parameter_3)))",
        whereClause);

    QueryContext ResearchSubjectContext =
        QueryHelper.getNewQueryContext(
            "all_Subjects_v3_0_final", "all_Files_v3_0_final", "ResearchSubject", "project", true);

    String rsWhere = query.buildQuery(ResearchSubjectContext);

    assertEquals(1, ResearchSubjectContext.getUnnests().size());

    ResearchSubjectContext.getUnnests()
        .forEach(
            unnest -> {
              if (unnest.getPath().equals("Subject.ResearchSubject")) {
                assertEquals(SqlUtil.JoinType.INNER, unnest.getJoinType());
              } else {
                assertEquals(SqlUtil.JoinType.LEFT, unnest.getJoinType());
                assertEquals("_ResearchSubject.Diagnosis", unnest.getPath());
              }
            });
  }
}
