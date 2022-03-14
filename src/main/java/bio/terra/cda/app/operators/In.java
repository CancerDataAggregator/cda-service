package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;

import java.util.Map;

@QueryOperator(nodeType = Query.NodeTypeEnum.IN)
public class In extends BasicOperator {
    @Override
    public String queryString(String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap) {
        String right = ((BasicOperator)getR()).queryString(table, tableSchemaMap);
        if (right.contains("[") || right.contains("(")) {
            right = right.substring(1, right.length() - 1).replace("\"", "'");
        } else {
            throw new IllegalArgumentException("To use IN you need to add [ or (");
        }

        String left = ((BasicOperator)getL()).queryString(table, tableSchemaMap);
        return String.format("(%s IN (%s))", left, right);
    }
}
