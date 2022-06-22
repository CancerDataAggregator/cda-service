package bio.terra.cda.app.util;

import bio.terra.cda.generated.model.Query;
import java.util.Objects;

public class QueryUtil {
  private QueryUtil() {}

  public static Query deSelectifyQuery(Query query) {
    return removeSelectsFromQuery(query);
  }

  private static Query removeSelectsFromQuery(Query currentQuery) {
    if (Objects.isNull(currentQuery)) {
      return null;
    }

    if (currentQuery.getNodeType().equals(Query.NodeTypeEnum.SELECT)) {
      return removeSelectsFromQuery(currentQuery.getR());
    } else {
      currentQuery.setL(removeSelectsFromQuery(currentQuery.getL()));
      currentQuery.setR(removeSelectsFromQuery(currentQuery.getR()));
      return currentQuery;
    }
  }
}
