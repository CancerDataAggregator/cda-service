package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.models.Select;
import bio.terra.cda.app.util.SqlUtil;

public class SelectBuilder {
  private final String table;
  private final String fileTable;

  public SelectBuilder(String table, String fileTable) {
    this.table = table;
    this.fileTable = fileTable;
  }

  public Select fromQueryField(QueryField queryField) {
    var parts = queryField.getParts();
    String alias = String.join("_", parts);
    String tbl = queryField.isFileField() ? fileTable : table;
    String field =
        String.format(
            SqlUtil.ALIAS_FIELD_FORMAT,
            parts.length == 1 ? tbl : SqlUtil.getAlias(parts.length - 2, parts),
            parts[parts.length - 1]);
    return new Select(field, alias);
  }
}
