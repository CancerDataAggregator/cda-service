package bio.terra.cda.app.operators;

import bio.terra.cda.generated.model.Operator;

@QueryOperator(nodeType = Operator.NodeTypeEnum.NOT_IN)
public class NotIn extends In {
}
