package bio.terra.cda.app.operators;

import bio.terra.cda.app.helpers.QueryFileReader;
import bio.terra.cda.app.helpers.QueryHelper;
import bio.terra.cda.app.util.QueryContext;
import java.io.IOException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SelectTest {
  @Test
  void testInvalidColumn() throws IOException {
    var query = QueryFileReader.getQueryFromFile("query-invalid-select-column.json");

    QueryContext ctx =
        QueryHelper.getNewQueryContext(
            "all_Subjects_v3_0_final", "all_Files_v3_0_final", "Subject", "project", true);


        assertDoesNotThrow(() -> ((ListOperator)query.getSelect().get(0)).buildQuery(ctx),"Did Not Expect Error");
    assertThrows(IllegalArgumentException.class, () -> ((ListOperator)query.getSelect().get(1)).buildQuery(ctx),"Expected query to throw IllegalArgumentException but didn't");

  }
//
//  @Test
//  void testSelectMultipleColumnsSameNestedObj() throws IOException {
//    BasicOperator query =
//        (BasicOperator) QueryFileReader.getQueryFromFile("query-select-easy.json");
//
//    QueryContext ctx =
//        QueryHelper.getNewQueryContext(
//            "all_Subjects_v3_0_final", "all_Files_v3_0_final", "Subject", "project", true);
//
//    String whereClause = query.buildQuery(ctx);
//
//    assertEquals(1, ctx.getUnnests().size());
//    assertEquals(3, ctx.getSelect().size());
//
//    if (ctx.getPartitions().stream()
//        .noneMatch(partition -> partition.toString().equals("_ResearchSubject.id"))) {
//      fail();
//    }
//  }
}
