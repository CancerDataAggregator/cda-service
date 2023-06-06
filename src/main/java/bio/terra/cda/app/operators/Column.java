package bio.terra.cda.app.operators;

import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;

import java.util.List;

@QueryOperator(nodeType = {Query.NodeTypeEnum.COLUMN})
public class Column extends BasicOperator {

  @Override
  public String buildQuery(QueryContext ctx) {

    QueryField queryField = ctx.getQueryFieldBuilder().fromPath(getValue());

    if (!queryField.getTableName().equals(ctx.getTable())) {
      ctx.addJoins(ctx.getJoinBuilder().getJoinsFromQueryField(ctx.getTable(), queryField));
    }

    var columnText = queryField.getName();

    BasicOperator parent = getParent();
    NodeTypeEnum nodeType = parent.getNodeType();
    return queryField.getType().equalsIgnoreCase("text")
            && !List.of(NodeTypeEnum.IS, NodeTypeEnum.IS_NOT).contains(nodeType)
        ? String.format("COALESCE(UPPER(%s), '')", columnText)
        : columnText;
  }

  @Override
  public String getSQLType(BasicOperator caller, QueryContext ctx) {
    return ctx.getQueryFieldBuilder().fromPath(getValue()).getType();
  }
}
