package bio.terra.cda.app.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import bio.terra.cda.app.operators.Column;
import bio.terra.cda.app.operators.Select;
import bio.terra.cda.generated.model.Query;
import java.util.List;
import org.junit.jupiter.api.Test;

public class QueryUtilTest {
  @Test
  void testDeSelectifyQuery() {

    Query query = new Query();
    query.setSelect(List.of(new Select().setOperator(new Column().setValue("test"))));
    Query newQuery = QueryUtil.deSelectifyQuery(query);

    assertEquals(0, newQuery.getSelect().size());
  }
}
