package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;

import java.util.Map;

@QueryOperator(nodeType = Query.NodeTypeEnum.LIKE)
public class Like extends SingleSidedOperator {
    @Override
    public String queryString(String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap) throws IllegalArgumentException {
        String rightValue = getR().getValue();
        String leftValue = ((BasicOperator)getL()).queryString(table, tableSchemaMap);
        return String.format("%s LIKE UPPER(%s)", leftValue,rightValue);
    }
}
