package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.util.Map;
import java.util.stream.Stream;

public class BasicOperator extends Query {
  public Stream<String> getUnnestColumns(
      String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap)
      throws IllegalArgumentException {
    return Stream.concat(
        ((BasicOperator) getL()).getUnnestColumns(table, tableSchemaMap),
        ((BasicOperator) getR()).getUnnestColumns(table, tableSchemaMap));
  }

  public String queryString(String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap)
      throws IllegalArgumentException {
    return String.format(
        "(%s %s %s)",
        ((BasicOperator) getL()).queryString(table, tableSchemaMap),
        this.getNodeType(),
        ((BasicOperator) getR()).queryString(table, tableSchemaMap));
  }
}
