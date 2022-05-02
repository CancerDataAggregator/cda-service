package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

@QueryOperator(nodeType = Query.NodeTypeEnum.SELECTVALUES)
public class SelectValues extends BasicOperator {
  @Override
  public Stream<String> getUnnestColumns(
      String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap, Boolean includeSelect)
      throws IllegalArgumentException {
    return Arrays.stream(getValue().split(","))
        .flatMap(select -> SqlUtil.getUnnestsFromParts(table, select.trim().split("\\."), false));
  }

  @Override
  public String queryString(String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap)
      throws IllegalArgumentException {
    return "";
  }
}
