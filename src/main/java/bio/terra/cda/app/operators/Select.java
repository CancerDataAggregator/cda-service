package bio.terra.cda.app.operators;



import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Operator;

import java.util.Objects;

@QueryOperator(nodeType = Operator.NodeTypeEnum.SELECT)
public class Select extends ListOperator {
    @Override
    public String buildQuery(QueryContext ctx) throws IllegalArgumentException {
        String modifier = this.getModifier();


        return Objects.isNull(modifier) || modifier.isEmpty()
                ? this.getOperator().buildQuery(ctx)
                : String.format("%s AS %s",this.getOperator().buildQuery(ctx),modifier);
    }

}
