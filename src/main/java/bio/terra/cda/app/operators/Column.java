package bio.terra.cda.app.operators;

import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Operator;
import com.google.cloud.bigquery.LegacySQLTypeName;
import java.util.List;
import java.util.Objects;

@QueryOperator(nodeType = Operator.NodeTypeEnum.COLUMN)
public class Column extends BasicOperator {

    @Override
    public String buildQuery(QueryContext ctx) {
        addUnnests(ctx);

        QueryField queryField = ctx.getQueryFieldBuilder().fromPath(getValue());

        var columnText = queryField.getColumnText();

        BasicOperator parent = getParent();

        String defaultValue = this.getDefaultValue();

        if (Objects.nonNull(parent)
                && List.of(NodeTypeEnum.IS, NodeTypeEnum.IS_NOT).contains(parent.getNodeType())) {
            return columnText;
        }

        String result = queryField.getType().equals(LegacySQLTypeName.STRING.toString())
                ? String.format("UPPER(%s)", columnText)
                : columnText;

        var parameterBuilder = ctx.getParameterBuilder();

        if (Objects.nonNull(defaultValue)) {
            result = String.format(
                    "IFNULL(%s, %s)",
                    result,
                    parameterBuilder.addParameterValue(queryField, defaultValue));
        }

        return result;
    }
}
