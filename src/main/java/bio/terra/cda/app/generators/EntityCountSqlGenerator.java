package bio.terra.cda.app.generators;

import bio.terra.cda.app.builders.ViewBuilder;
import bio.terra.cda.app.models.*;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.QueryUtil;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.generated.model.Query;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EntityCountSqlGenerator extends SqlGenerator {
  protected List<ColumnDefinition> totalCountFields;
  protected List<ColumnDefinition> groupedCountFields;

  public EntityCountSqlGenerator(Query rootQuery, boolean filesQuery) {
    super(rootQuery, filesQuery);
  }

  @Override
  protected void initializeEntityFields() {
    CountQueryGenerator queryGenerator = this.getClass().getAnnotation(CountQueryGenerator.class);

    if (queryGenerator == null) {
      throw new RuntimeException("No entity table annotation found");
    }
    this.entityTable = this.dataSetInfo.getTableInfo(queryGenerator.entity());

    totalCountFields =
        Arrays.stream(queryGenerator.totalFieldsToCount())
            .map(
                field ->
                    dataSetInfo.getColumnDefinitionByFieldName(
                        field, this.entityTable.getTableName()))
            .collect(Collectors.toList());

    groupedCountFields =
        Arrays.stream(queryGenerator.groupedFieldsToCount())
            .map(
                field ->
                    dataSetInfo.getColumnDefinitionByFieldName(
                        field, this.entityTable.getTableName()))
            .collect(Collectors.toList());
  }

  @Override
  protected String sql(
      String tableOrSubClause,
      Query query,
      boolean subQuery,
      boolean hasSubClause,
      boolean ignoreWith) {
    String viewSql =
        super.sql(
            tableOrSubClause, QueryUtil.deSelectifyQuery(query), subQuery, hasSubClause, true);
    String tableAlias = "flattened_result";
    this.viewListBuilder.addView(new ManualView(String.format("%s as (%s)", tableAlias, viewSql)));
    addGroupedCountViews(tableAlias);
//    String withStatement = "";
//    if (this.viewListBuilder.hasAny() && !ignoreWith) {
//      withStatement = String.format("%s, %s as (%s)", getWithStatement(), tableAlias, viewSql);
//    } else {
//      withStatement = String.format("WITH %s as (%s)", tableAlias, viewSql);
//    }

    return subQuery ? viewSql : String.format("%s select %s", getWithStatement(), getCountSelects(tableAlias));
  }

  protected void addGroupedCountViews(String tableAlias) {
    groupedCountFields.forEach(
                    col -> addEachGroupedCountView(col, tableAlias));
  }

  protected void addEachGroupedCountView(ColumnDefinition col, String fromTableAlias) {
    String fieldName = col.getAlias();

    String groupedCountInnerView = String.format(
        "(select %1$s as %1$s, count(distinct %2$s) as count from %3$s group by %1$s)",
        fieldName,
        this.entityTable.getPrimaryKeysAlias().get(0),
        fromTableAlias);

    String viewNameFormatString = "%s_count";
    String viewSelectFormatString =  "json_%s";
    ViewBuilder viewBuilder = viewListBuilder.getViewBuilder();
    viewBuilder
        .setViewName(String.format(viewNameFormatString, fieldName))
        .setViewType(View.ViewType.WITH)
        .addSelect(new Select("row_to_json(subq)", String.format(viewSelectFormatString, fieldName)))
        .setFromClause(groupedCountInnerView, "subq");
    View view = viewBuilder.build();
    viewListBuilder.addView(view);

  }
  // race_count as (select  row_to_json(subq) as json_race from
  // (select race as race, count(distinct subject_id) as count from flattened_result group by race)
  // as subq),

  protected ColumnDefinition getSecondaryEntity() {
    return null;
  }


  protected String getCountSelects(String tableAlias) {
    String totalFormatString = "(SELECT COUNT(DISTINCT %s) from %s) as %s";
    String groupedFormatString =
        "(SELECT array_agg(json_%1$s) from %1$s_count) as %1$s";

    List<ColumnDefinition> totalFields = totalCountFields;
    if (filesQuery) {
      ColumnDefinition entityCol = getSecondaryEntity();
      if (entityCol != null) {
        totalFields.add(getSecondaryEntity());
        }
    }
    return Stream.concat(
        totalCountFields.stream()
                .map(
                    col ->
                        String.format(
                            totalFormatString, col.getAlias(), tableAlias, col.getAlias())),
            groupedCountFields.stream()
                .map(
                    col ->
                        String.format(
                            groupedFormatString,
                            col.getAlias())))
        .collect(Collectors.joining(", "));
  }

  @Override
  protected Stream<String> getSelectsFromEntity(
      QueryContext ctx, List<ColumnDefinition> additionalColumns, Map<ColumnDefinition, String> aggregateFieldsAndSql) {

    List<ColumnDefinition> totalFields = totalCountFields;
    if (filesQuery) {
      ColumnDefinition entityCol = getSecondaryEntity();
      if (entityCol != null) {
        totalFields.add(getSecondaryEntity());
      }
    }

    return Stream.concat(totalFields.stream(), this.groupedCountFields.stream())
        .map(
            col -> {
              // if we need to find a path to the attribute
              if (!this.entityTable.getTableName().equals(col.getTableName())) {
                List<Join> path =
                    ctx.getJoinBuilder()
                        .getPath(
                            this.entityTable.getTableName(), col.getTableName(), col.getName());
                ctx.addJoins(path);
              }
              return String.format(
                  "%1$s.%2$s AS %3$s", col.getTableName(), col.getName(), col.getAlias());
            });
  }
}
