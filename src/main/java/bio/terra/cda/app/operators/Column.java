package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.util.Map;
import java.util.stream.Stream;

@QueryOperator(nodeType = Query.NodeTypeEnum.COLUMN)
public class Column extends BasicOperator {
  @Override
  public Stream<String> getUnnestColumns(
      String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap, Boolean includeSelect) {
    try {
      var tmp = tableSchemaMap.get(getValue());
      var tmpGetMode = tmp.getMode();
      var parts = getValue().split("\\.");
      return SqlUtil.getUnnestsFromParts(table, parts, (tmpGetMode.equals("REPEATED")));
    } catch (NullPointerException e) {
      throw new IllegalArgumentException(
          String.format("Column %s does not exist on table %s", getValue(), table));
    }
  }

  @Override
  public String queryString(
      String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap) {
    var tmp = tableSchemaMap.get(getValue());
    var tmpGetMode = tmp.getMode();
    var tmpGetType = tmp.getType();
    var value = getValue();
    var parts = value.split("\\.");
    var columnText = "";
    if (tmpGetMode.equals("REPEATED")) {
      columnText = String.format("%s", SqlUtil.getAlias(parts.length - 1, parts));
    } else if (parts.length == 1) {
      columnText = String.format("%s.%s", table, value);
    } else {
      columnText =
          String.format(
              "%s.%s", SqlUtil.getAlias(parts.length - 2, parts), parts[parts.length - 1]);
    }

    return tmpGetType.equals("STRING") ? String.format("UPPER(%s)", columnText) : columnText;
  }
}
