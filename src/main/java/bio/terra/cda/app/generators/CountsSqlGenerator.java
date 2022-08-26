package bio.terra.cda.app.generators;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.operators.Select;
import bio.terra.cda.app.operators.SelectValues;
import bio.terra.cda.app.util.QueryUtil;
import bio.terra.cda.generated.model.Query;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CountsSqlGenerator extends SqlGenerator {
  public CountsSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version, false);
  }

  @Override
  protected String sql(String tableOrSubClause, Query query, boolean subQuery)
          throws UncheckedExecutionException, IllegalArgumentException {
    Map<String, TableInfo> tableInfoMap = new HashMap<>();

    getQueryGeneratorClasses()
        .forEach(
            clazz -> {
              var annotation = clazz.getAnnotation(QueryGenerator.class);
              TableInfo tableInfo = this.dataSetInfo.getTableInfo(annotation.entity());

              tableInfoMap.put(annotation.entity(), tableInfo);
            });

    // Add a select node to completely flatten out the result set
    Query newQuery =
        new Select()
            .nodeType(Query.NodeTypeEnum.SELECT)
            .l(
                new SelectValues()
                    .nodeType(Query.NodeTypeEnum.SELECTVALUES)
                    .value(
                        tableInfoMap.keySet().stream()
                            .map(key -> tableInfoMap.get(key).getPartitionKeyFullName(this.dataSetInfo))
                            .collect(Collectors.joining(","))))
            .r(QueryUtil.deSelectifyQuery(query));

    try {
      String resultsAlias = "flattened_results";
      return String.format(
          "%s SELECT %s FROM %s",
          String.format(
              "with %s as (%s)",
              resultsAlias,
              new SqlGenerator(
                      this.qualifiedTable, newQuery, this.version, false, this.parameterBuilder)
                  .sql(this.qualifiedTable, newQuery, false)),
          tableInfoMap.keySet().stream().map(key -> {
                    TableInfo tableInfo = tableInfoMap.get(key);
                      String entityPartitionKey = tableInfo.getPartitionKey();
                      if (Objects.isNull(this.dataSetInfo.getSchemaDefinitionByFieldName(entityPartitionKey))) {
                          entityPartitionKey = DataSetInfo.getNewNameForDuplicate(entityPartitionKey, tableInfo.getTableName());
                      }

                    return String.format(
                        "COUNT(DISTINCT %s) AS %s",
                        entityPartitionKey, String.format("%s_count", key.toLowerCase()));
                  }).collect(Collectors.joining(", ")),
          resultsAlias);
    } catch (IOException e) {
      throw new UncheckedExecutionException(e);
    }
  }
}
