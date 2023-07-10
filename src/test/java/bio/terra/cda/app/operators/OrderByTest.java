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
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class OrderByTest {
  @Test
  void testInvalidColumn() throws IOException {
    Query query = QueryFileReader.getQueryFromFile("query-invalid-order-by-column.json");

    SqlGenerator sqlgen = new SqlGenerator(query, false);
    TableInfo subjectTableInfo = RdbmsSchema.getDataSetInfo().getTableInfo("subject");
    QueryContext ctx = sqlgen.buildQueryContext(subjectTableInfo, false, false);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> query.getOrderBy().forEach(e -> ((ListOperator) e).buildQuery(ctx)),
            "Expected query to throw IllegalArgumentException but didn't");
  }

  @Test
  void testOrderByMultipleColumnsSameNestedObj() throws IOException {
    var query = QueryFileReader.getQueryFromFile("query-orderby.json");

    SqlGenerator sqlgen = new SqlGenerator(query, false);
    TableInfo subjectTableInfo = RdbmsSchema.getDataSetInfo().getTableInfo("subject");
    QueryContext ctx = sqlgen.buildQueryContext(subjectTableInfo, false, false);
    String OrderByClause =
        query.getOrderBy().stream()
            .map(e -> ((ListOperator) e).buildQuery(ctx))
            .collect(Collectors.joining(", "));

    assertEquals(
        "UPPER(id) ASC, COALESCE(UPPER(primary_diagnosis_site), '') ASC, COALESCE(UPPER(primary_diagnosis_condition), '') DESC",
        OrderByClause);
  }
}
