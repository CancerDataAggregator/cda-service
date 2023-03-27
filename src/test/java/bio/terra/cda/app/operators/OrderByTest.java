package bio.terra.cda.app.operators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import bio.terra.cda.app.helpers.QueryFileReader;
import bio.terra.cda.app.helpers.QueryHelper;
import bio.terra.cda.app.helpers.StorageServiceHelper;
import bio.terra.cda.app.helpers.TableSchemaHelper;
import bio.terra.cda.app.models.OrderBy;
import bio.terra.cda.app.service.StorageService;
import bio.terra.cda.app.util.QueryContext;
import java.io.IOException;
import java.util.List;

import bio.terra.cda.app.util.TableSchema;
import org.junit.jupiter.api.Test;

public class OrderByTest {
  @Test
  void testInvalidColumn() throws IOException {
    BasicOperator query =
        (BasicOperator) QueryFileReader.getQueryFromFile("query-invalid-select-column.json");
    TableSchema tableSchema = TableSchemaHelper.getNewTableSchema("v3");

    QueryContext ctx =
        QueryHelper.getNewQueryContext(
                tableSchema, "all_Subjects_v3_0_final", "all_Files_v3_0_final", "Subject", "project", true);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> query.buildQuery(ctx),
            "Expected query to throw IllegalArgumentException but didn't");
  }

  @Test
  void testOrderByMultipleColumnsSameNestedObj() throws IOException {
    BasicOperator query = (BasicOperator) QueryFileReader.getQueryFromFile("query-orderby.json");
    TableSchema tableSchema = TableSchemaHelper.getNewTableSchema("v3");

    QueryContext ctx =
        QueryHelper.getNewQueryContext(
                tableSchema, "all_Subjects_v3_0_final", "all_Files_v3_0_final", "Subject", "project", true);

    String whereClause = query.buildQuery(ctx);

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
