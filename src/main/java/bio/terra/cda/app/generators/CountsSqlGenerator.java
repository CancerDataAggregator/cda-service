package bio.terra.cda.app.generators;

import bio.terra.cda.app.models.RdbmsSchema;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.operators.Select;
import bio.terra.cda.app.operators.SelectValues;
import bio.terra.cda.app.util.EndpointUtil;
import bio.terra.cda.generated.model.Query;
import java.util.*;
import java.util.stream.Collectors;

public class CountsSqlGenerator extends SqlGenerator {
  public CountsSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
    this.entityTable = RdbmsSchema.getDataSetInfo().getEntityTableInfo("subject");
  }

  @Override
  protected String sql(
      String tableOrSubClause,
      Query query,
      boolean subQuery,
      boolean hasSubClause,
      boolean ignoreWith) {
    List<String> primaryKeyFields = new ArrayList<>();

    EndpointUtil.getQueryGeneratorClasses()
        .forEach(
            clazz -> {
              var annotation = clazz.getAnnotation(QueryGenerator.class);
              TableInfo tableInfo = this.dataSetInfo.getTableInfo(annotation.entity());
              if (this.entityTable == null) {
                this.entityTable = tableInfo;
              }

              // this can be removed when we have the mutations table in postgres
              if (tableInfo != null) {
                primaryKeyFields.addAll(tableInfo.getPrimaryKeysAlias());
              }
            });

    // Add a select node to completely flatten out the result set
    Query newQuery = new Query();
    newQuery.setSelect(
        primaryKeyFields.stream()
            .map(keyField -> new Select().setOperator(new SelectValues().setValue(keyField)))
            .collect(Collectors.toList()));
    newQuery.setWhere(query.getWhere());
    String resultsAlias = "flattened_results";
    String flattenedWith =
        String.format(
            "%s as (%s)",
            resultsAlias,
            new SqlGenerator(newQuery, false, this.parameterBuilder, this.viewListBuilder)
                .sql(this.entityTable.getTableName(), newQuery, false, false, true));
    String withStatement = String.format("WITH %s", flattenedWith);

    if (this.viewListBuilder.hasAny() && !ignoreWith) {
      withStatement = String.format("%s, %s", getWithStatement(), flattenedWith);
    }
    return String.format(
        "%s SELECT %s FROM %s",
        withStatement,
        primaryKeyFields.stream()
            .map(
                field ->
                    String.format(
                        "COUNT(DISTINCT %s) AS %s",
                        field, String.format("%s_count", field.toLowerCase())))
            .collect(Collectors.joining(", ")),
        resultsAlias);
  }
}
