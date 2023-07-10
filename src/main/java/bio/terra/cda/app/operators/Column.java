package bio.terra.cda.app.operators;

import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Operator;
import java.util.List;
import java.util.Objects;

@QueryOperator(nodeType = Operator.NodeTypeEnum.COLUMN)
public class Column extends BasicOperator {
  private boolean ignoreDefault = false;

  @Override
  public String buildQuery(QueryContext ctx) {

    QueryField queryField = ctx.getQueryFieldBuilder().fromPath(getValue());

    if (!queryField.getTableName().equals(ctx.getTable())) {
      ctx.addJoins(ctx.getJoinBuilder().getJoinsFromQueryField(ctx.getTable(), queryField));
    }

    var columnText = queryField.getName();

    BasicOperator parent = getParent();

    if (Objects.nonNull(parent)
        && List.of(NodeTypeEnum.IS, NodeTypeEnum.IS_NOT).contains(parent.getNodeType())) {
      return columnText;
    }

    String defaultValue = this.getDefaultValue();

    boolean isTextField = queryField.getType().equalsIgnoreCase("text");

    String result = isTextField ? String.format("UPPER(%s)", columnText) : columnText;

    if (!this.ignoreDefault) {
      var parameterBuilder = ctx.getParameterBuilder();

      if (isTextField && Objects.isNull(defaultValue)) {
        result = String.format("COALESCE(%s, '')", result);
      } else {
        result =
            String.format(
                "COALESCE(%s, %s)",
                result, parameterBuilder.addParameterValue(queryField, defaultValue));
      }
    }

    return result;
  }

  @Override
  public String getSQLType(BasicOperator caller, QueryContext ctx) {
    return ctx.getQueryFieldBuilder().fromPath(getValue()).getType();
  }

  public boolean getIgnoreDefault() {
    return this.ignoreDefault;
  }

  public Column setIgnoreDefault(boolean ignore) {
    this.ignoreDefault = ignore;
    return this;
  }
}
