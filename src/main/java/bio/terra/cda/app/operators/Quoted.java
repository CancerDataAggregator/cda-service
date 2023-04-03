package bio.terra.cda.app.operators;


import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Operator;

@QueryOperator(nodeType = Operator.NodeTypeEnum.QUOTED)
public class Quoted extends BasicOperator {

    @Override
    public String buildQuery(QueryContext ctx) {
        var parameterBuilder = ctx.getParameterBuilder();
        String parameterName = parameterBuilder.addParameterValue(ctx.getQueryFieldBuilder()
                .fromPath(((BasicOperator) this.getParent().getLeft()).getValue()), getValue());
        return String.format("UPPER(%s)", parameterName);
    }
}
