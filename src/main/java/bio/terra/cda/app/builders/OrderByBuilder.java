package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.OrderBy;
import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.models.RdbmsSchema;

public class OrderByBuilder {

  DataSetInfo dataSetInfo;
  public OrderByBuilder() {
    dataSetInfo = RdbmsSchema.getDataSetInfo();
  }

  public OrderBy fromQueryField(QueryField queryField) {
    return new OrderBy(dataSetInfo.getColumnDefinitionByFieldName(
        queryField.getName(), queryField.getTableName()),
        OrderBy.OrderByModifier.valueOf(queryField.getModifier()));
  }
}
