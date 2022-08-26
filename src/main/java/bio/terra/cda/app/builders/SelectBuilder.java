package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.models.Select;
import bio.terra.cda.app.util.SqlUtil;
import com.google.cloud.bigquery.Field;

public class SelectBuilder {
  private final String table;
  private final String fileTable;

  public SelectBuilder(String table, String fileTable) {
    this.table = table;
    this.fileTable = fileTable;
  }

  public Select fromQueryField(QueryField queryField) {
    String field = queryField.getColumnText();
    if (queryField.getMode().equals(Field.Mode.REPEATED.toString())) {
      var parts = queryField.getParts();
      if (parts.length == 1) {
        field =
            String.format(
                "%s.%s", queryField.isFileField() ? fileTable : table, queryField.getName());
      } else {
        field =
            String.format("%s.%s", SqlUtil.getAlias(parts.length - 2, parts), queryField.getName());
      }
    }
    return new Select(field, queryField.getAlias());
  }
}
