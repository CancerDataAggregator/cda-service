package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;

import java.util.Map;

@QueryOperator(nodeType = Query.NodeTypeEnum.NOT)
public class Not extends SingleSidedOperator {
    @Override
    public String queryString(String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap) {
        return String.format("(%s %s)", getNodeType(), ((BasicOperator)getL()).queryString(table, tableSchemaMap));
    }
}
