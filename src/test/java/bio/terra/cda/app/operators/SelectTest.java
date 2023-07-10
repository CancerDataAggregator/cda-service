package bio.terra.cda.app.operators;

import static org.junit.jupiter.api.Assertions.*;

import bio.terra.cda.app.generators.SqlGenerator;
import bio.terra.cda.app.helpers.QueryFileReader;
import bio.terra.cda.app.models.RdbmsSchema;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Operator;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SelectTest {
  @Test
  void testInvalidColumn() throws IOException {
    Query query = QueryFileReader.getQueryFromFile("query-invalid-select-column.json");

    SqlGenerator sqlgen = new SqlGenerator(query, false);
    TableInfo subjectTableInfo = RdbmsSchema.getDataSetInfo().getTableInfo("subject");
    QueryContext ctx = sqlgen.buildQueryContext(subjectTableInfo, false, false);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> ((ListOperator) query.getSelect().get(1)).buildQuery(ctx),
            "Expected query to throw IllegalArgumentException but didn't");
  }

  @Test
  void testSelectMultipleColumnsSameNestedObj() throws IOException {
    Query query = new Query();
    query.setSelect(
        List.of(
            new Select().setOperator(new SelectValues().setValue("subject_id")),
            new Select().setOperator(new SelectValues().setValue("primary_diagnosis_site")),
            new Select().setOperator(new SelectValues().setValue("primary_diagnosis_condition"))));
    query.setWhere(
        new Operator()
            .nodeType(Operator.NodeTypeEnum.EQUAL)
            .l(new Column().setValue("subject_id"))
            .r(new Quoted().setValue("test")));
    SqlGenerator sqlgen = new SqlGenerator(query, false);
    TableInfo subjectTableInfo = RdbmsSchema.getDataSetInfo().getTableInfo("subject");
    QueryContext ctx = sqlgen.buildQueryContext(subjectTableInfo, false, false);
    query.getSelect().forEach(lo -> ((ListOperator) lo).buildQuery(ctx));

    assertEquals(2, ctx.getJoins().size());
    assertEquals(3, ctx.getSelect().size());

    //    if (ctx.getSelect().stream()
    //        .noneMatch(partition -> partition.toString().equals("research_subject_id"))) {
    //      fail();
    //    }
  }
}
