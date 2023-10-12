package bio.terra.cda.app.operators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.cda.app.generators.EntitySqlGenerator;
import bio.terra.cda.app.generators.SqlGenerator;
import bio.terra.cda.app.helpers.QueryFileReader;
import bio.terra.cda.app.models.OrderBy;
import bio.terra.cda.app.models.RdbmsSchema;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.util.QueryContext;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

public class OrderByTest {
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
  void testOrderByMultipleColumnsSameNestedObj() throws IOException {
    BasicOperator query = (BasicOperator) QueryFileReader.getQueryFromFile("query-orderby.json");

    EntitySqlGenerator sqlgen = new EntitySqlGenerator(query, false);
    TableInfo subjectTableInfo = RdbmsSchema.getDataSetInfo().getTableInfo("subject");
    QueryContext ctx = sqlgen.buildQueryContext(subjectTableInfo, false, false);
    String sqlStr = query.buildQuery(ctx);

    assertEquals(3, ctx.getOrderBys().size());

    List<OrderBy> orderByList = ctx.getOrderBys();
    for (var orderBy : orderByList) {
      if (orderBy.getFieldName().equals("id")) {
        assertEquals(OrderBy.OrderByModifier.ASC, orderBy.getModifier());
      } else if (orderBy.getFieldName().equals("primary_diagnosis_site")) {
        assertEquals(OrderBy.OrderByModifier.ASC, orderBy.getModifier());
      } else {
        assertEquals(OrderBy.OrderByModifier.DESC, orderBy.getModifier());
      }
    }
  }
}
