package bio.terra.cda.app.operators;

import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;
import com.google.cloud.bigquery.LegacySQLTypeName;

@QueryOperator(nodeType = {Query.NodeTypeEnum.COLUMN})
public class Column extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx) {
    addUnnests(ctx);

    QueryField queryField = ctx.getQueryFieldBuilder().fromPath(getValue());

    var columnText = queryField.getColumnText();

    return queryField.getType().equals(LegacySQLTypeName.STRING.toString())
            ? String.format("UPPER(%s)", columnText) : columnText;
  }
}
