package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Operator;

import java.util.List;
import java.util.Objects;

public class BasicOperator extends Operator {
    private BasicOperator parent;
    private String value;
    private List<BasicOperator> operators;
    private boolean isNullable;

    public BasicOperator(){
        QueryOperator operator = this.getClass().getAnnotation(QueryOperator.class);
        this.isNullable = false;
        if (Objects.nonNull(operator)){
            this.setNodeType(operator.nodeType());
        }
    }
    public BasicOperator(Operator.NodeTypeEnum nodeType) {
        this.setNodeType(nodeType);
    }




    public String buildQuery(QueryContext ctx) {
        return String.format("(%s %s %s)", ((BasicOperator) getLeft()).buildQuery(ctx),
                this.getNodeType(), ((BasicOperator) getRight()).buildQuery(ctx));
    }

    public BasicOperator setParent(BasicOperator operator) {
        this.parent = operator;
        return this;
    }

    protected BasicOperator getParent() {
        return parent;
    }

    protected void addUnnests(QueryContext ctx) {
        ctx.addUnnests(ctx.getUnnestBuilder()
                .fromQueryField(ctx.getQueryFieldBuilder().fromPath(getValue()), true));
    }

    public BasicOperator setOperators(List<BasicOperator> operators) {
        this.operators = operators;
        return this;
    }

    public List<BasicOperator> getOperators() {
        return operators;
    }

    public BasicOperator setValue(String value) {
        this.value = value;
        return this;
    }

    public String getValue() {
        return this.value;
    }
    private String toIndentedString(Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();


        sb.append(String.format("class %s {\n",this.getClass().getName()));
        sb.append("    nodeType: ").append(toIndentedString(this.getNodeType())).append("\n");
        sb.append("    operators: ").append(toIndentedString(this.getOperators())).append("\n");
        sb.append("    value: ").append(toIndentedString(this.getValue())).append("\n");
        sb.append("    modifier: ").append(toIndentedString(this.getModifier())).append("\n");
        sb.append("    left: ").append(toIndentedString(this.getLeft())).append("\n");
        sb.append("    right: ").append(toIndentedString(this.getRight())).append("\n");
        sb.append("}");
        return sb.toString();
    }

    public boolean isNullable() {
        return isNullable;
    }

    public BasicOperator setNullable(boolean nullable) {
        isNullable = nullable;
        return this;
    }
}

