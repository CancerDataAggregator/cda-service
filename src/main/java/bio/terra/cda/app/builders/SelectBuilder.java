package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.models.Select;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;

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
    String tbl = table;
    String fieldAlias = SqlUtil.getAlias(parts.length - 2, parts);

    if (queryField.isFileField()) {
      tbl = fileTable;
      alias = String.format("%s_%s", TableSchema.FILE_PREFIX, alias);
      fieldAlias = String.format("_%s%s", TableSchema.FILE_PREFIX, fieldAlias);
    }

    String field =
        String.format(
            SqlUtil.ALIAS_FIELD_FORMAT,
            parts.length == 1 ? tbl : fieldAlias,
            parts[parts.length - 1]);
    return new Select(field, alias);
  }
}
