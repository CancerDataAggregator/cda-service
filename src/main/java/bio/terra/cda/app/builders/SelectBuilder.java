package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.*;
import bio.terra.cda.app.util.SqlUtil;
import org.springframework.beans.factory.annotation.Autowired;

public class SelectBuilder {
  @Autowired RdbmsSchema rdbmsSchema;

  private final DataSetInfo dataSetInfo;

  public SelectBuilder() {
    this.dataSetInfo = rdbmsSchema.getDataSetInfo();
  }

  public Select fromQueryField(QueryField queryField) {
    TableInfo tableInfo = dataSetInfo.getTableInfoFromField(queryField.getPath());

    String field =
        String.format(
            SqlUtil.ALIAS_FIELD_FORMAT,
            tableInfo.getTableAlias(this.dataSetInfo),
            queryField.getName());

    return new Select(field, queryField.getAlias());
  }

  public Select of(String field, String alias) {
    return new Select(field, alias);
  }
}
