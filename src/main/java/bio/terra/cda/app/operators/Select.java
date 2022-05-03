package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.util.Map;
import java.util.stream.Stream;

@QueryOperator(nodeType = Query.NodeTypeEnum.SELECT)
public class Select extends BasicOperator {
  @Override
  public Stream<String> getUnnestColumns(
      String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap, Boolean includeSelect)
      throws IllegalArgumentException {
    return includeSelect
        ? super.getUnnestColumns(table, tableSchemaMap, includeSelect)
        : ((BasicOperator) getR()).getUnnestColumns(table, tableSchemaMap, includeSelect);
  }

  @Override
  public String queryString(String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap)
      throws IllegalArgumentException {
    return ((BasicOperator) getR()).queryString(table, tableSchemaMap);
  }
}
