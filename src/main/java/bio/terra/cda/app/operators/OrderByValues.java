package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.generated.model.Query;
import com.google.cloud.bigquery.Field;

import java.util.Arrays;

@QueryOperator(nodeType = Query.NodeTypeEnum.ORDERBYVALUES)
public class OrderByValues extends BasicOperator {
    @Override
    public String buildQuery(QueryContext ctx) throws IllegalArgumentException {
        addUnnests(ctx);
        addOrderBys(ctx);
        addPartitions(ctx);
        return "";
    }

    @Override
    public void addUnnests(QueryContext ctx) {
        ctx.addUnnests(
                Arrays.stream(getValue().split(","))
                        .map(String::trim)
                        .map(ctx.getQueryFieldBuilder()::fromPath)
                        .filter(field -> field.getMode().equals(Field.Mode.REPEATED.toString()))
                        .flatMap(field -> ctx.getUnnestBuilder().fromQueryField(field, true)));
    }

    private void addOrderBys(QueryContext ctx) {
        ctx.addOrderBys(
                Arrays.stream(getValue().split(","))
                        .map(String::trim)
                        .map(ctx.getQueryFieldBuilder()::fromPath)
                        .map(ctx.getOrderByBuilder()::fromQueryField));
    }

    private void addPartitions(QueryContext ctx) {
        ctx.addPartitions(
                Arrays.stream(getValue().split(","))
                        .map(String::trim)
                        .map(ctx.getQueryFieldBuilder()::fromPath)
                        .map(ctx.getPartitionBuilder()::fromQueryField));
    }
}
