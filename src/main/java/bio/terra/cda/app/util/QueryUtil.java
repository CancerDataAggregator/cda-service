package bio.terra.cda.app.util;

import static java.lang.Integer.parseInt;

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

  public static Query removeLimitOROffest(Query query, QueryContext ctx) {

    return removeLimitOffest(query, ctx);
  }

  private static Query removeLimitOffest(Query currentQuery, QueryContext ctx) {

    if (Objects.isNull(currentQuery)) {
      return null;
    }
    if (currentQuery.getNodeType().equals(Query.NodeTypeEnum.LIMIT)) {
      ctx.setLimit(parseInt(currentQuery.getValue()));
      return removeLimitOffest(currentQuery.getR(), ctx);
    }
    if (currentQuery.getNodeType().equals(Query.NodeTypeEnum.OFFSET)) {
      ctx.setOffset(parseInt(currentQuery.getValue()));

      return removeLimitOffest(currentQuery.getR(), ctx);
    }
    currentQuery.setL(removeLimitOffest(currentQuery.getL(), ctx));
    currentQuery.setR(removeLimitOffest(currentQuery.getR(), ctx));

    return currentQuery;
  }
}
