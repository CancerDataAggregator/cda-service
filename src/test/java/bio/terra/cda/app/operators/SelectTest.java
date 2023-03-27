package bio.terra.cda.app.operators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import bio.terra.cda.app.helpers.QueryFileReader;
import bio.terra.cda.app.helpers.QueryHelper;
import bio.terra.cda.app.helpers.StorageServiceHelper;
import bio.terra.cda.app.helpers.TableSchemaHelper;
import bio.terra.cda.app.service.StorageService;
import bio.terra.cda.app.util.QueryContext;
import java.io.IOException;

import bio.terra.cda.app.util.TableSchema;
import org.junit.jupiter.api.Test;

public class SelectTest {
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
  void testSelectMultipleColumnsSameNestedObj() throws IOException {
    BasicOperator query =
        (BasicOperator) QueryFileReader.getQueryFromFile("query-select-easy.json");
    TableSchema tableSchema = TableSchemaHelper.getNewTableSchema("v3");

    QueryContext ctx =
        QueryHelper.getNewQueryContext(
                tableSchema, "all_Subjects_v3_0_final", "all_Files_v3_0_final", "Subject", "project", true);

    String whereClause = query.buildQuery(ctx);

    assertEquals(1, ctx.getUnnests().size());
    assertEquals(3, ctx.getSelect().size());

    if (ctx.getPartitions().stream()
        .noneMatch(partition -> partition.toString().equals("_ResearchSubject.id"))) {
      fail();
    }
  }
}
