package bio.terra.cda.app.operators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import bio.terra.cda.app.generators.EntitySqlGenerator;
import bio.terra.cda.app.generators.SqlGenerator;
import bio.terra.cda.app.models.RdbmsSchema;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.helpers.QueryFileReader;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class SelectTest {
  @Test
  void testInvalidColumn() throws IOException {
    BasicOperator query =
        (BasicOperator) QueryFileReader.getQueryFromFile("query-invalid-select-column.json");

    EntitySqlGenerator sqlgen = new EntitySqlGenerator(query, false);
    TableInfo subjectTableInfo = RdbmsSchema.getDataSetInfo().getTableInfo("subject");
    QueryContext ctx = sqlgen.buildQueryContext(subjectTableInfo, false, false);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> query.buildQuery(ctx),
            "Expected query to throw IllegalArgumentException but didn't");
  }

  @Test
  void testSelectMultipleColumnsSameNestedObj() throws IOException {
    BasicOperator query =
        (BasicOperator) QueryFileReader.getQueryFromFile("query-select-easy.json");

    EntitySqlGenerator sqlgen = new EntitySqlGenerator(query, false);
    TableInfo subjectTableInfo = RdbmsSchema.getDataSetInfo().getTableInfo("subject");
    QueryContext ctx = sqlgen.buildQueryContext(subjectTableInfo, false, false);

    query.buildQuery(ctx);

    assertEquals(2, ctx.getJoins().size());
    assertEquals(3, ctx.getSelect().size());

//    if (ctx.getSelect().stream()
//        .noneMatch(partition -> partition.toString().equals("research_subject_id"))) {
//      fail();
//    }
  }
}
