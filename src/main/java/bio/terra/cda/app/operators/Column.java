package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.generated.model.Query;

@QueryOperator(nodeType = {Query.NodeTypeEnum.COLUMN})
public class Column extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx) {
    addUnnests(ctx);

    var value = getValue();
    var isFileField = value.toLowerCase().startsWith("file.");
    var schemaMap = isFileField ? ctx.getFileTableSchemaMap() : ctx.getTableSchemaMap();
    if (isFileField) {
      value = value.substring(value.indexOf(".") + 1);
    }

    var tmp = schemaMap.get(value);
    var tmpGetMode = tmp.getMode();
    var tmpGetType = tmp.getType();
    var parts = SqlUtil.getParts(value);
    var columnText = "";
    if (tmpGetMode.equals("REPEATED")) {
      columnText = String.format("%s", SqlUtil.getAlias(parts.length - 1, parts));
    } else if (parts.length == 1) {
      columnText = String.format("%s.%s", isFileField ? ctx.getFileTable() : ctx.getTable(), value);
    } else {
      columnText =
          String.format(
              "%s.%s", SqlUtil.getAlias(parts.length - 2, parts), parts[parts.length - 1]);
    }

    return tmpGetType.equals("STRING") ? String.format("UPPER(%s)", columnText) : columnText;
  }
}
