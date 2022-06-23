package bio.terra.cda.app.operators;

import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;
import com.google.cloud.bigquery.LegacySQLTypeName;
import java.util.List;

@QueryOperator(nodeType = {Query.NodeTypeEnum.COLUMN})
public class Column extends BasicOperator {
  @Override
  public String buildQuery(QueryContext ctx) {
    addUnnests(ctx);

    QueryField queryField = ctx.getQueryFieldBuilder().fromPath(getValue());

    var columnText = queryField.getColumnText();

    BasicOperator parent = getParent();
    NodeTypeEnum nodeType = parent.getNodeType();
    return queryField.getType().equals(LegacySQLTypeName.STRING.toString())
            && !List.of(NodeTypeEnum.IS, NodeTypeEnum.IS_NOT).contains(nodeType)
        ? String.format("IFNULL(UPPER(%s), '')", columnText)
        : columnText;
  }
}
