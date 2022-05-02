package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.util.Map;
import java.util.stream.Stream;

@QueryOperator(nodeType = Query.NodeTypeEnum.QUOTED)
public class Quoted extends BasicOperator {
  @Override
  public Stream<String> getUnnestColumns(
      String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap, Boolean includeSelect) {
    return Stream.empty();
  }

  @Override
  public String queryString(
      String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap) {
    String value = getValue();
    // Int check
    if (value.contains("days_to_birth")
        || value.contains("age_at_death")
        || value.contains("age_")) {
      return String.format("'%s'", value);
    }
    return String.format("UPPER('%s')", value);
  }
}
