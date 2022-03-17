package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.util.Map;

@QueryOperator(nodeType = Query.NodeTypeEnum.SELECT)
public class Select extends BasicOperator {
  @Override
  public String queryString(String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap)
      throws IllegalArgumentException {
    return ((BasicOperator) getR()).queryString(table, tableSchemaMap);
  }
}
