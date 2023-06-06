package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.OrderBy;
import bio.terra.cda.app.models.QueryField;

public class OrderByBuilder {

  public OrderByBuilder() {}

  public OrderBy fromQueryField(QueryField queryField) {
    return new OrderBy(
        queryField.getName(),
        queryField.getAlias(),
        OrderBy.OrderByModifier.valueOf(queryField.getModifier()));
  }
}
