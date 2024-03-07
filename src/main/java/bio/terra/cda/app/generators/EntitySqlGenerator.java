package bio.terra.cda.app.generators;

import bio.terra.cda.app.builders.*;
import bio.terra.cda.app.models.*;
import bio.terra.cda.app.operators.BasicOperator;
import bio.terra.cda.app.util.*;
import bio.terra.cda.generated.model.Query;
import com.google.common.base.Strings;
import org.springframework.data.relational.core.mapping.Table;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class EntitySqlGenerator extends SqlGenerator {

  final Query rootQuery;
  final DataSetInfo dataSetInfo;
  final boolean filesQuery;

  Map<ColumnDefinition, String> aggregatedFieldsAndSelectString = new LinkedHashMap<>();
  SelectBuilder selectBuilder = new SelectBuilder();
  QueryFieldBuilder queryFieldBuilder = new QueryFieldBuilder(false);

  QueryFieldBuilder filesQueryFieldBuilder = new  QueryFieldBuilder(true);

  JoinBuilder joinBuilder = new JoinBuilder();
  ViewListBuilder<View, ViewBuilder> viewListBuilder = new ViewListBuilder<>(ViewBuilder.class);
  OrderByBuilder orderByBuilder = new OrderByBuilder();

  OrderBy defaultOrderBy;
  TableInfo entityTable;



  public EntitySqlGenerator(Query rootQuery, boolean filesQuery) {
    this.rootQuery = rootQuery;
    this.filesQuery = filesQuery;
    this.dataSetInfo = RdbmsSchema.getDataSetInfo();


    preInit();
    initializeEntityFields();
  }

  public EntitySqlGenerator(
      Query rootQuery,
      boolean filesQuery,
      ParameterBuilder parameterBuilderToUse,
      ViewListBuilder<View, ViewBuilder>  viewListBuilder) {
    this(rootQuery, filesQuery);

    // This statement should be invalid (want something like super.parameterBuilder or create a set method
    // parameterBuilder = parameterBuilder;
    parameterBuilder = parameterBuilderToUse;

    this.viewListBuilder = viewListBuilder;
  }

  protected void preInit() {
    // nothing here, meant for subclasses to override
    // and add preInit tasks before EntityFields and builders are initialized
  }

  protected void initializeEntityFields() {
    EntityGeneratorData entityGeneratorData = this.getClass().getAnnotation(EntityGeneratorData.class);

    if (entityGeneratorData != null) {
      this.entityTable = this.dataSetInfo.getTableInfo(entityGeneratorData.entity());

      String defaultOrderByField = entityGeneratorData.defaultOrderBy();
      if (!Strings.isNullOrEmpty(defaultOrderByField)) {
        defaultOrderBy = orderByBuilder.fromQueryField(queryFieldBuilder.fromPath(defaultOrderByField));
      }

      List<ColumnDefinition> colList = Arrays.stream(entityGeneratorData.aggregatedFields())
            .map(dataSetInfo::getColumnDefinitionByFieldName)
            .collect(Collectors.toList());
      List<String> selectList = Arrays.asList(entityGeneratorData.aggregatedFieldsSelectString());
      this.aggregatedFieldsAndSelectString = IntStream.range(0, colList.size())
          .boxed()
          .collect(Collectors.toMap(colList::get, selectList::get));
    } else {
      this.entityTable = this.dataSetInfo.getEntityTableInfo("subject");
    }
  }

  public QueryContext buildQueryContext(
      TableInfo entityTable, boolean filesQuery) {
    return new QueryContext(entityTable.getTableName())
        .setFilesQuery(filesQuery)
        .setTableInfo(entityTable)
        .setIncludeSelect(true)
        .setQueryFieldBuilder(filesQuery ? filesQueryFieldBuilder : queryFieldBuilder)
        .setSelectBuilder(selectBuilder)
        .setJoinBuilder(joinBuilder)
        .setParameterBuilder(parameterBuilder)
        .setOrderByBuilder(orderByBuilder)
        .setViewListBuilder(viewListBuilder);
  }

  protected String generate() throws IllegalArgumentException {
      return sql(entityTable.getTableName(), rootQuery, false);
  }



  protected String sql(
      String table,
      Query query,
      boolean ignoreWith)
      throws IllegalArgumentException {
    QueryContext ctx = buildQueryContext(this.entityTable, filesQuery);

    String results = resultsQuery(query, ctx);

    String withStatement = "";
    if (this.viewListBuilder.hasAny() && !ignoreWith) {
      withStatement = getWithStatement();
    }

    return String.format("%s%s", withStatement, results);
  }


  protected String resultsQuery(
      Query query,
      QueryContext ctx) {
    TableInfo startTable = this.entityTable;

    String condition = ((BasicOperator) query).buildQuery(ctx);
    String selectFields = getSelect(ctx).collect(Collectors.joining(", "));

    var fromClause =
        Stream.concat(
            Stream.of(
                    String.format(
                        "%s AS %s",
                        startTable.getTableName(),
                        startTable.getTableAlias(this.dataSetInfo))),
            ctx.getJoins().stream().map(SqlTemplate::join));

    String fromString = fromClause.distinct().collect(Collectors.joining(" "));

    String orderBys = ctx.getOrderBys().stream().map(OrderBy::toString).collect(Collectors.joining(", "));
    if (Strings.isNullOrEmpty(orderBys) && !Objects.isNull(defaultOrderBy)) {
      orderBys = defaultOrderBy.toString();
    }
    ctx.addOrderBysToGroupBys();

    return SqlTemplate.resultsQuery(
        selectFields,
        fromString,
        condition,
        ctx.getGroupBys(),
        orderBys);
  }

  protected Stream<String> getSelect(QueryContext ctx) {
    if (!ctx.getSelect().isEmpty()) {
      return ctx.getSelect().stream().map(Select::toString);
    } else {
      return getSelectsFromEntity(
          ctx,
          this.filesQuery ? FileSqlGenerator.getExternalFieldsAndSqlString() :
          getAggregatedFieldsAndSelectString());
    }
  }

  protected Map<ColumnDefinition, String> getAggregatedFieldsAndSelectString() {
    return aggregatedFieldsAndSelectString;
  }

  protected Stream<String> getSelectsFromEntity(
      QueryContext ctx, Map<ColumnDefinition, String> aggregateFields) {

    Set<ColumnDefinition> totalExternalColumns = new HashSet<>(aggregateFields.keySet());
    totalExternalColumns.forEach(col -> {
      if (!this.entityTable.getTableName().equals(col.getTableName())) {
        List<Join> path =
            ctx.getJoinBuilder()
                .getPath(
                    this.entityTable.getTableName(), col.getTableName(), col.getName());
        ctx.addJoins(path);
      }
    });

    Set<ColumnDefinition> columns = new LinkedHashSet<>();

    if (filesQuery) {
      columns.addAll(this.entityTable.getPrimaryKeys());
      columns.addAll(Arrays.asList(dataSetInfo.getTableInfo("file").getColumnDefinitions()));
    } else {
      columns.addAll(Arrays.asList(this.entityTable.getColumnDefinitions()));
    }

    return Stream.concat(
        columns.stream().filter(col -> !col.getName().endsWith("_id_alias"))
            .map(
                col -> {
                    ctx.addGroupBy(col);
                  return String.format(
                      "%1$s.%2$s AS %3$s", col.getTableName(), col.getName(), col.getAlias());
                }),
        aggregateFields.values().stream());
  }


  protected String getWithStatement() {
    return String.format(
        "WITH %s ",
        this.viewListBuilder.build().stream()
            .map(View::toString)
            .collect(Collectors.joining(", ")));
  }

  public JoinBuilder getJoinBuilder(){
    return this.joinBuilder;
  }

  public String getEntityTableName(){
    return this.entityTable.getTableName();
  }
  public String getEntityTableFirstPK(){
    List<ColumnDefinition> pkcols = this.entityTable.getPrimaryKeys();
    return pkcols.isEmpty() ? "" : this.entityTable.getPrimaryKeys().get(0).getName();
  }
  public DataSetInfo getDataSetInfo(){
    return this.dataSetInfo;
  }
  public TableInfo getEntityTable() { return this.entityTable; }
}
