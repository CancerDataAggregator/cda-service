package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.generated.model.Query;

import java.util.Arrays;

@QueryOperator(nodeType = Query.NodeTypeEnum.ORDER)
public class Order extends BasicOperator {
    @Override
    public String buildQuery(QueryContext ctx) {
        addUnnests(ctx);
        addOrderBy(ctx);

        return "";
    }

    @Override
    protected void addUnnests(QueryContext ctx) {
        try{
            Arrays.stream(getValue().split(",")).map(String::trim)
                    .forEach(
                            orderBy -> {
                                var parts = Arrays.stream(orderBy.split("\\s+")).map(String::trim).toArray(String[]::new);
                                var tmp = ctx.getTableSchemaMap().get(parts[0]);
                                var tmpGetMode = tmp.getMode();
                                var columnParts =
                                        Arrays.stream(parts[0].split("\\.")).map(String::trim).toArray(String[]::new);
                                ctx.addUnnests(SqlUtil.getUnnestsFromParts(ctx.getTable(), columnParts, (tmpGetMode.equals("REPEATED"))));
                            });
        } catch (NullPointerException e) {
            throw new IllegalArgumentException(
                    String.format("Column %s does not exist on table %s", getValue(), ctx.getTable()));
        }
    }

    private void addOrderBy(QueryContext ctx) {
        try {
            Arrays.stream(getValue().split(",")).map(String::trim)
                    .forEach(
                            orderBy -> {
                                var parts = Arrays.stream(orderBy.split("\\s+")).map(String::trim).toArray(String[]::new);
                                var columnParts =
                                        Arrays.stream(parts[0].split("\\.")).map(String::trim).toArray(String[]::new);
                                ctx.addOrderBy(String.format(
                                        "%s.%s%s",
                                        columnParts.length == 1 ? ctx.getTable() : SqlUtil.getAlias(columnParts.length - 2, columnParts),
                                        columnParts[columnParts.length - 1],
                                        parts.length > 1 ? String.format(" %s", parts[1]) : ""));
                            });
        } catch (NullPointerException e) {
            throw new IllegalArgumentException(
                    String.format("Column %s does not exist on table %s", getValue(), ctx.getTable()));
        }
    }
}
