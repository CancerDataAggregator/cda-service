package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SqlGenerator {
    final String qualifiedTable;
    final Query rootQuery;
    final String table;
    final Map<String, TableSchema.SchemaDefinition> tableSchemaMap;
    final List<TableSchema.SchemaDefinition> tableSchema;

    public SqlGenerator(String qualifiedTable, Query rootQuery, String version) throws IOException {
        this.qualifiedTable = qualifiedTable;
        this.rootQuery = rootQuery;
        int dotPos = qualifiedTable.lastIndexOf('.');
        this.table = dotPos == -1 ? qualifiedTable : qualifiedTable.substring(dotPos + 1);
        this.tableSchema = TableSchema.getSchema(version);
        this.tableSchemaMap = TableSchema.buildSchemaMap(this.tableSchema);
    }

    public String generate() {
        return sql(qualifiedTable, rootQuery);
    }

    protected String sql(String tableOrSubClause, Query query) {
        if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
            // A SUBQUERY is built differently from other queries. The FROM clause is the
            // SQL version of
            // the right subtree, instead of using table. The left subtree is now the top
            // level query.
            return sql(String.format("(%s)", sql(tableOrSubClause, query.getR())), query.getL());
        }

        var fromClause = Stream.concat(
                Stream.of(tableOrSubClause + " AS " + table), getUnnestColumns(query).distinct())
                .collect(Collectors.joining(", "));

        String condition = null;
        try {
            condition = queryString(query);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return String.format("%s FROM %s WHERE %s", getSelect(query, table), fromClause, condition);
    }

    protected String getSelect(Query query, String table) {
        if (query.getNodeType() == Query.NodeTypeEnum.SELECT) {
            return String.format("SELECT %s", queryToSelect(query).collect(Collectors.joining(",")));
        } else {
            return String.format("SELECT %s.*", table);
        }
    }

    protected Stream<String> queryToSelect(Query query) {
        return Arrays.stream(query.getL().getValue().split(",")).map(select -> {
            var parts = Arrays.stream(select.split("\\.")).map(String::trim).toArray(String[]::new);
            return String.format("%s.%s AS %s",
                    parts.length == 1
                            ? table
                            : getAlias(parts.length - 2, parts),
                    parts[parts.length - 1],
                    String.join("_", parts));
        });
    }

    protected Stream<String> getUnnestColumns(Query query) {
        switch (query.getNodeType()) {
            case SELECTVALUES:
                return Arrays.stream(query.getValue().split(","))
                        .flatMap(select -> getUnnestsFromParts(select.trim().split("\\."), false));
            case LIKE:
                return getUnnestColumns(query.getL());
            case QUOTED:
            case UNQUOTED:
                return Stream.empty();
            case COLUMN:
               var tmp = tableSchemaMap.get(query.getValue());
               var tmpGetMode = tmp.getMode();
               var tmpGetType = tmp.getType();
                if (tmpGetMode.equals("REPEATED") && tmpGetType.equals("STRING")){
                   String tableValue = String.format("_%s",query.getValue());
                   Stream<String> tmpReturn = Arrays.stream(new String[]{String.format("UNNEST(%1$s) AS %2$s", query.getValue(), tableValue)});
                    return tmpReturn;
               }
                var parts = query.getValue().split("\\.");
                return getUnnestsFromParts(parts, false);
            case NOT:
                return getUnnestColumns(query.getL());
            default:
                return Stream.concat(getUnnestColumns(query.getL()), getUnnestColumns(query.getR()));
        }
    }

    protected String queryString(Query query) throws IllegalArgumentException {
        switch (query.getNodeType()) {
            case SELECT:
                return queryString(query.getR());
            case SELECTVALUES:
                return "";
            case QUOTED:
                String value = query.getValue();
                // Int check
                if (value.contains("days_to_birth") || value.contains("age_at_death") || value.contains("age_")) {
                    return String.format("'%s'", value);
                }
                return String.format("UPPER('%s')", value);
            case UNQUOTED:
                return String.format("%s", query.getValue());
            case COLUMN:
                var tmp = tableSchemaMap.get(query.getValue());
                var tmpGetMode = tmp.getMode();
                var tmpGetType = tmp.getType();
                if (tmpGetMode.equals("REPEATED") && tmpGetType.equals("STRING")){
                    String tableValue = String.format("_%s",query.getValue());
                    return String.format("UPPER(%s)", tableValue);
                }

                var parts = query.getValue().split("\\.");
                if (parts.length > 1) {
                    // int check for values that are a int so the UPPER function will not run
                    if (parts[parts.length - 1].contains("age_")) {
                        return String.format("%s.%s", getAlias(parts.length - 2, parts), parts[parts.length - 1]);
                    }
                    return String.format("UPPER(%s.%s)", getAlias(parts.length - 2, parts), parts[parts.length - 1]);
                }
                // Top level fields must be scoped by the table name, otherwise they could
                // conflict with
                // unnested fields.
                String value_col = query.getValue();
                if (value_col.contains("days_to_birth") || value_col.contains("age_at_death")) {
                    return String.format("%s.%s", table, value_col);
                }
                return String.format("UPPER(%s.%s)", table, query.getValue());
            case NOT:
                return String.format("(%s %s)", query.getNodeType(), queryString(query.getL()));
            case IN:
                String right = queryString(query.getR());
                if (right.contains("[") || right.contains("(")) {
                    right = right.substring(1, right.length() - 1).replace("\"", "'");
                } else {
                    throw new IllegalArgumentException("To use IN you need to add [ or (");
                }

                String left = queryString(query.getL());
                return String.format("(%s IN (%s))", left, right);

            case LIKE:
                String rightValue = query.getR().getValue();
                String leftValue = queryString(query.getL());
                return String.format("%s LIKE UPPER(%s)", leftValue,rightValue);
            default:
                return String.format(
                        "(%s %s %s)",
                        queryString(query.getL()), query.getNodeType(), queryString(query.getR()));
        }
    }

    protected Stream<String> getUnnestsFromParts(String[] parts, Boolean includeLast) {
        return IntStream.range(0, parts.length - (includeLast ? 0 : 1))
                .mapToObj(
                        i -> i == 0
                                ? String.format("UNNEST(%1$s.%2$s) AS %3$s", table, parts[i], getAlias(i, parts))
                                : String.format("UNNEST(%1$s.%2$s) AS %3$s", getAlias(i - 1, parts), parts[i],
                                        getAlias(i, parts)));
    }

    protected String getAlias(Integer index, String[] parts) {
        return "_" + Arrays.stream(parts, 0, index + 1).collect(Collectors.joining("_"));
    }
}
