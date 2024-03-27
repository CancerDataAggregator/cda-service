package bio.terra.cda.app.generators;

import bio.terra.cda.app.builders.ViewBuilder;
import bio.terra.cda.app.models.*;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.QueryUtil;
import bio.terra.cda.generated.model.Query;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EntityCountSqlGenerator extends EntitySqlGenerator {
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

    String entity = filesQuery ? "file" : queryGenerator.entity();
    this.entityTable = this.dataSetInfo.getEntityTableInfo(entity);

    String[] totalFieldsToCount = filesQuery ? FileCountSqlGenerator.getTotalFieldsToCount() : queryGenerator.totalFieldsToCount();
    totalCountFields =
        Arrays.stream(totalFieldsToCount)
            .map(
                field ->
                    dataSetInfo.getColumnDefinitionByFieldName(
                        field, this.entityTable.getTableName()))
            .collect(Collectors.toList());

    String[] groupedFieldsToCount = filesQuery ? FileCountSqlGenerator.getGroupedFieldsToCount() : queryGenerator.groupedFieldsToCount();
    groupedCountFields =
        Arrays.stream(groupedFieldsToCount)
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
      boolean ignoreWith) {
    String viewSql =
        super.sql(
            tableOrSubClause, QueryUtil.deSelectifyQuery(query), true).replace("SELECT", "SELECT DISTINCT");
    String tableAlias = "flattened_result";
    this.viewListBuilder.addView(new ManualView(String.format("%s as (%s)", tableAlias, viewSql)));
    addGroupedCountViews(tableAlias);
    return String.format("%s select %s", getWithStatement(), getCountSelects(tableAlias));
  }

  protected void addGroupedCountViews(String tableAlias) {
    groupedCountFields.forEach(
                    col -> addEachGroupedCountView(col, tableAlias));
  }

  protected void addEachGroupedCountView(ColumnDefinition col, String fromTableAlias) {
    String fieldName = col.getAlias();
    String groupedCountInnerView = "";
    if (this.entityTable.getTableName().equals("somatic_mutation")){
      groupedCountInnerView = String.format(
              "(select %1$s as %1$s, count(*) as count from %2$s group by %1$s)",
              fieldName,
              fromTableAlias);
    } else {
      groupedCountInnerView = String.format(
              "(select %1$s as %1$s, count(distinct %2$s) as count from %3$s group by %1$s)",
              fieldName,
              this.entityTable.getPrimaryKeysAlias().get(0),
              fromTableAlias);
    }


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

  protected String getTotalFormatString(){
    String totalFormatString = "";
    if (this.entityTable.getTableName().equals("somatic_mutation")){
      totalFormatString = "(SELECT COUNT(*) from %2$s) as %1$s";
    } else {
      totalFormatString = "(SELECT COUNT(DISTINCT %1$s) from %2$s) as %1$s";
    }
    return totalFormatString;
  }
  protected String getCountSelects(String tableAlias) {
    String totalFormatString = getTotalFormatString();

    String groupedFormatString =
        "(SELECT array_agg(json_%1$s) from %1$s_count) as %1$s";

    List<ColumnDefinition> totalFields = totalCountFields;
    if (filesQuery) {
      ColumnDefinition entityCol = getSecondaryEntity();
      if (entityCol != null) {
        totalFields.add(getSecondaryEntity());
        }
    }
    String test = Stream.concat(
          totalCountFields.stream()
              .filter(Objects::nonNull)
              .map(col -> String.format(totalFormatString, replaceAliasWithId(col.getAlias()), tableAlias)),
          groupedCountFields.stream()
              .filter(Objects::nonNull)
              .map(col -> String.format(groupedFormatString, col.getAlias())))
        .collect(Collectors.joining(", "));
    return test;
  }


  protected Stream<String> getSelectsFromEntity(
      QueryContext ctx, Map<ColumnDefinition, String> aggregateFieldsAndSql) {

    List<ColumnDefinition> totalFields = totalCountFields;
    if (filesQuery) {
      ColumnDefinition entityCol = getSecondaryEntity();
      if (entityCol != null) {
        totalFields.add(getSecondaryEntity());
      }
    }

    return Stream.concat(totalFields.stream(), this.groupedCountFields.stream())
        .filter (Objects::nonNull)
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
                  "%1$s.%2$s AS %3$s", col.getTableName(), col.getName(), replaceAliasWithId(col.getAlias()));
            });
  }

  protected String replaceAliasWithId(String integerAliasAlias) {
    return integerAliasAlias.replace("alias", "id");
  }
  public List<ColumnDefinition> getTotalCountFields(){
    return this.totalCountFields;
  }
  public List<ColumnDefinition> getGroupedCountFields(){
    return this.groupedCountFields;
  }
}
