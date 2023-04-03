package bio.terra.cda.app.operators;

import java.util.List;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Operator;

@QueryOperator(nodeType = Operator.NodeTypeEnum.UNQUOTED)
public class Unquoted extends BasicOperator {


    @Override
    public String buildQuery(QueryContext ctx) throws IllegalArgumentException {
        String value = getValue();

        if (value.equalsIgnoreCase("null")) {
            return value;
        }

        var parameterBuilder = ctx.getParameterBuilder();
        return parameterBuilder.addParameterValue(ctx.getQueryFieldBuilder()
                .fromPath(((BasicOperator) this.getParent().getLeft()).getValue()), value);
    }
}
