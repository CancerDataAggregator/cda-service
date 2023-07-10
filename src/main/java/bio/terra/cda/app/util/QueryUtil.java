package bio.terra.cda.app.util;

import bio.terra.cda.generated.model.Query;
import java.util.List;

public class QueryUtil {

  private QueryUtil() {}

  public static Query deSelectifyQuery(Query query) {
    query.setSelect(List.of());
    return query;
  }
}
