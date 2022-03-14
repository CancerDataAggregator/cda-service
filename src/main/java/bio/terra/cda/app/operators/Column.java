package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;

import java.util.Map;
import java.util.stream.Stream;

@QueryOperator(nodeType = Query.NodeTypeEnum.COLUMN)
public class Column extends BasicOperator {
    @Override
    public Stream<String> getUnnestColumns(String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap) {
        try {
            var tmp = tableSchemaMap.get(getValue());
            var tmpGetMode = tmp.getMode();
            var tmpGetType = tmp.getType();
            var parts = getValue().split("\\.");
            return SqlUtil.getUnnestsFromParts(table, parts, (tmpGetMode.equals("REPEATED") && tmpGetType.equals("STRING")));


//                return getUnnestsFromParts(parts, false);
        }catch (NullPointerException e){
            throw new NullPointerException(String.format("Column %s does not exist on table %s",getValue(), table));
        }
    }
    @Override
    public String queryString(String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap) {
        var tmp = tableSchemaMap.get(getValue());
        var tmpGetMode = tmp.getMode();
        var tmpGetType = tmp.getType();
        if (tmpGetMode.equals("REPEATED") && tmpGetType.equals("STRING")){
            var splitQuery = getValue().split("\\.");
            String tableValue = String.format("%s",SqlUtil.getAlias(splitQuery.length-1,splitQuery));
            return String.format("UPPER(%s)", tableValue);
        }

        var parts = getValue().split("\\.");
        if (parts.length > 1) {
            // int check for values that are a int so the UPPER function will not run
            if (parts[parts.length - 1].contains("age_")) {
                return String.format("%s.%s", SqlUtil.getAlias(parts.length - 2, parts), parts[parts.length - 1]);
            }
            return String.format("UPPER(%s.%s)", SqlUtil.getAlias(parts.length - 2, parts), parts[parts.length - 1]);
        }
        // Top level fields must be scoped by the table name, otherwise they could
        // conflict with
        // unnested fields.
        String value_col = getValue();
        if (value_col.contains("days_to_birth") || value_col.contains("age_at_death")) {
            return String.format("%s.%s", table, value_col);
        }
        return String.format("UPPER(%s.%s)", table, getValue());
    }
}
