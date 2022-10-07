package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.models.Select;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.util.SqlUtil;

public class SelectBuilder {
  private final DataSetInfo dataSetInfo;

  public SelectBuilder(DataSetInfo dataSetInfo) {
    this.dataSetInfo = dataSetInfo;
  }

  public Select fromQueryField(QueryField queryField) {
    TableInfo tableInfo = dataSetInfo.getTableInfoFromField(queryField.getPath());

    String field =
        String.format(SqlUtil.ALIAS_FIELD_FORMAT, tableInfo.getTableAlias(this.dataSetInfo), queryField.getName());

    return new Select(field, queryField.getAlias());
  }

  public Select of(String field, String alias) {
    return new Select(field, alias);
  }
}
