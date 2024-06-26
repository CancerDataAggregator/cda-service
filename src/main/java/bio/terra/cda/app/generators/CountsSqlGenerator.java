package bio.terra.cda.app.generators;

import bio.terra.cda.app.models.RdbmsSchema;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.operators.Select;
import bio.terra.cda.app.operators.SelectValues;
import bio.terra.cda.app.util.EndpointUtil;
import bio.terra.cda.app.util.QueryUtil;
import bio.terra.cda.generated.model.Query;
import java.util.*;
import java.util.stream.Collectors;

public class CountsSqlGenerator extends EntitySqlGenerator {
  public CountsSqlGenerator(Query rootQuery) {
    super(rootQuery, false);
    this.entityTable = RdbmsSchema.getDataSetInfo().getEntityTableInfo("subject");
  }

  @Override
  protected String sql(
      String tableOrSubClause,
      Query query,
      boolean ignoreWith) {
    List<String> primaryKeyFields = new ArrayList<>();

    EndpointUtil.getQueryGeneratorClasses()
        .forEach(
            clazz -> {
              var annotation = clazz.getAnnotation(EntityGeneratorData.class);
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
    Query newQuery =
        new Select()
            .nodeType(Query.NodeTypeEnum.SELECT)
            .l(
                new SelectValues()
                    .nodeType(Query.NodeTypeEnum.SELECTVALUES)
                    .value(String.join(",", primaryKeyFields)))
            .r(QueryUtil.deSelectifyQuery(query));
//TODO: EntitySQLGenerator -> Build out new structure of optimized query
    String resultsAlias = "flattened_results";
    String flattenedWith =
        String.format(
            "%s as (%s)",
            resultsAlias,
            new EntitySqlGenerator(newQuery, false, this.parameterBuilder, this.viewListBuilder)
                .sql(this.entityTable.getTableName(), newQuery, true));
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
