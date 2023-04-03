package bio.terra.cda.app.operators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.cda.app.helpers.QueryFileReader;
import bio.terra.cda.app.helpers.QueryHelper;
import bio.terra.cda.app.models.OrderBy;
import bio.terra.cda.app.util.QueryContext;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

public class OrderByTest {
  @Test
  void testInvalidColumn() throws IOException {
    var query = QueryFileReader.getQueryFromFile("query-invalid-order-by-column.json");

    QueryContext ctx =
        QueryHelper.getNewQueryContext(
            "all_Subjects_v3_0_final", "all_Files_v3_0_final", "Subject", "project", true);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> query.getOrderBy().forEach(e -> ( (ListOperator) e).buildQuery(ctx)),
            "Expected query to throw IllegalArgumentException but didn't");
  }

  @Test
  void testOrderByMultipleColumnsSameNestedObj() throws IOException {
    var query =  QueryFileReader.getQueryFromFile("query-orderby.json");

    QueryContext ctx =
        QueryHelper.getNewQueryContext(
            "all_Subjects_v3_0_final", "all_Files_v3_0_final", "Subject", "project", true);

    String OrderByClause = query.getOrderBy().stream().map(e -> ((ListOperator) e).buildQuery(ctx)).collect(Collectors.joining(", "));
    assertEquals("IFNULL(UPPER(Subject.id), '') ASC, IFNULL(UPPER(_ResearchSubject.primary_diagnosis_site), '') ASC, IFNULL(UPPER(_ResearchSubject.primary_diagnosis_condition), '') DESC"
            ,OrderByClause);
  }
}
