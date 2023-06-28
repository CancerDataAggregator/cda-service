package bio.terra.cda.app.generators;

import bio.terra.cda.app.builders.*;
import bio.terra.cda.app.models.*;
import bio.terra.cda.app.operators.BasicOperator;
import bio.terra.cda.app.operators.ListOperator;
import bio.terra.cda.app.util.*;
import bio.terra.cda.generated.model.Operator;
import bio.terra.cda.generated.model.Query;
import com.google.common.base.Strings;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class SqlGenerator {

  final Query rootQuery;
  final DataSetInfo dataSetInfo;
  final boolean filesQuery;

  Map<ColumnDefinition, String> aggregatedFieldsAndSelectString = new LinkedHashMap<>();
  boolean modularEntity;
  SelectBuilder selectBuilder = new SelectBuilder();
  QueryFieldBuilder queryFieldBuilder = new QueryFieldBuilder(false);

  QueryFieldBuilder filesQueryFieldBuilder = new  QueryFieldBuilder(true);
  ParameterBuilder parameterBuilder = new ParameterBuilder();

  JoinBuilder joinBuilder = new JoinBuilder();
  ViewListBuilder<View, ViewBuilder> viewListBuilder = new ViewListBuilder<>(ViewBuilder.class);
  OrderByBuilder orderByBuilder = new OrderByBuilder();

  OrderBy defaultOrderBy;
  TableInfo entityTable;

  String querySql;


  public SqlGenerator(Query rootQuery, boolean filesQuery) {
    this.rootQuery = rootQuery;
    this.filesQuery = filesQuery;
    this.dataSetInfo = RdbmsSchema.getDataSetInfo();


    preInit();
    initializeEntityFields();
  }

  public SqlGenerator(
      Query rootQuery,
      boolean filesQuery,
      ParameterBuilder parameterBuilder,
      ViewListBuilder<View, ViewBuilder>  viewListBuilder) {
    this(rootQuery, filesQuery);

    this.parameterBuilder = parameterBuilder;
    this.viewListBuilder = viewListBuilder;
  }

  protected void preInit() {
    // nothing here, meant for subclasses to override
    // and add preInit tasks before EntityFields and builders are initialized
  }

  protected void initializeEntityFields() {
    QueryGenerator queryGenerator = this.getClass().getAnnotation(QueryGenerator.class);
//    this.modularEntity = queryGenerator != null;

    if (queryGenerator != null) {
      this.entityTable = this.dataSetInfo.getTableInfo(queryGenerator.entity());

      String defaultOrderByField = queryGenerator.defaultOrderBy();
      if (!Strings.isNullOrEmpty(defaultOrderByField)) {
        defaultOrderBy = orderByBuilder.fromQueryField(queryFieldBuilder.fromPath(defaultOrderByField));
      }
      List<ColumnDefinition> colList = Arrays.stream(queryGenerator.aggregatedFields())
            .map(field -> dataSetInfo.getColumnDefinitionByFieldName(field))
            .collect(Collectors.toList());
      List<String> selectList = Arrays.asList(queryGenerator.aggregatedFieldsSelectString());
      this.aggregatedFieldsAndSelectString = IntStream.range(0, colList.size())
          .boxed()
          .collect(Collectors.toMap(colList::get, selectList::get));
    } else {
      this.entityTable = this.dataSetInfo.getEntityTableInfo("subject");
    }
  }

  public QueryContext buildQueryContext(
      TableInfo entityTable, boolean filesQuery, boolean subQuery) {
    return new QueryContext(entityTable.getTableName())
        .setFilesQuery(filesQuery)
        .setTableInfo(entityTable)
        .setIncludeSelect(!subQuery)
        .setQueryFieldBuilder(queryFieldBuilder)
        .setSelectBuilder(selectBuilder)
        .setJoinBuilder(joinBuilder)
        .setParameterBuilder(parameterBuilder)
        .setOrderByBuilder(orderByBuilder)
        .setViewListBuilder(viewListBuilder);
  }

  protected String generate() throws IllegalArgumentException {
      return sql(entityTable.getTableName(), rootQuery, false, false, false);
  }

  public String getSqlString() {
    if (Strings.isNullOrEmpty(this.querySql)) {
      this.querySql = generate();
    }
    return this.querySql;
  }

  protected String sql(
      String tableOrSubClause,
      Query query,
      boolean subQuery,
      boolean hasSubClause,
      boolean ignoreWith)
      throws IllegalArgumentException {
    QueryContext ctx = buildQueryContext(this.entityTable, filesQuery, subQuery);

    String results = resultsQuery(query, tableOrSubClause, subQuery, ctx, hasSubClause);

    String withStatement = "";
    if (this.viewListBuilder.hasAny() && !ignoreWith) {
      withStatement = getWithStatement();
    }

    return String.format("%s%s", withStatement, results);
  }

  public String getReadableQuerySql() {
    // TODO should we be adding the offset and limit to the readable sql query?
    return this.parameterBuilder.substituteForReadableString(getSqlString());
  }

  public String getReadableQuerySql(Integer offset, Integer limit) {
    // TODO should we be adding the offset and limit to the readable sql query?

    return SqlTemplate.addPagingFields(getReadableQuerySql(), offset, limit);
  }


  public MapSqlParameterSource getNamedParameterMap() {
    return this.parameterBuilder.getParameterValueMap();
  }

  protected String resultsQuery(
      Query query,
      String tableOrSubClause,
      boolean subQuery,
      QueryContext ctx,
      boolean hasSubClause) {
    TableInfo tableInfo = ctx.getTableInfo();
    TableInfo startTable = this.entityTable;

    if (query.getWhere().getNodeType() == Operator.NodeTypeEnum.SUBQUERY) {
      // A SUBQUERY is built differently from other queries. The FROM clause is the
      // SQL version of
      // the right subtree, instead of using table. The left subtree is now the top
      // level query.

      Query left = new Query();
      Query right = new Query();

      left.setWhere(query.getWhere().getL());
      right.setWhere(query.getWhere().getR());

      return resultsQuery(
          left,
          String.format(
              "(%s) as %s",
              sql(tableOrSubClause, right, true, hasSubClause, true),
              startTable.getTableAlias(this.dataSetInfo)),
          subQuery,
          buildQueryContext(
              ctx.getTableInfo(), filesQuery, subQuery), // added  supertable to get parent
          true);
    }

    String condition = ((BasicOperator) query.getWhere()).buildQuery(ctx);
    String selectFields =
        subQuery
            ? ""
            : getSelect(ctx)
                .collect(Collectors.joining(", "));

    String orderByFields = query.getOrderBy()
            .stream().map(ob ->((ListOperator) ob).buildQuery(ctx))
            .collect(Collectors.joining(", "));

    var fromClause =
        Stream.concat(
            hasSubClause
                ? Stream.of(tableOrSubClause)
                : Stream.of(
                    String.format(
                        "%s AS %s",
                        startTable.getTableName(),
                        startTable.getTableAlias(this.dataSetInfo))),
            ctx.getJoins().stream().flatMap(joins -> joins.stream().map(join -> SqlTemplate.join(join))));

    String fromString = fromClause.distinct().collect(Collectors.joining(" "));

    String orderBys = ctx.getOrderBys().stream().map(OrderBy::toString).collect(Collectors.joining(", "));
    if (Strings.isNullOrEmpty(orderBys) && !Objects.isNull(defaultOrderBy)) {
      orderBys = defaultOrderBy.toString();
    }
    if (subQuery) {
      return SqlTemplate.regularQuery(
              String.format("%s.*", startTable.getTableAlias(this.dataSetInfo)),
              fromString,
              condition,
              orderBys);
    }

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

    aggregateFields.keySet().forEach(col -> {
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
        columns.stream()
            .map(
                col -> {
                    ctx.addGroupBy(col);
                  return String.format(
                      "%1$s.%2$s AS %3$s", col.getTableName(), col.getName(), col.getAlias());
                }),
        aggregateFields.values().stream());
  }

//  protected Stream<String> combinedSelects(
//      QueryContext ctx, String prefix, Stream<String> idSelects) {
//    TableInfo fileTableInfo = this.dataSetInfo.getTableInfo(RdbmsSchema.FILE_TABLE);
////    List<String> fileFilteredFields =
////        Arrays.stream(fileTableInfo.getColumnDefinitions())
////            .filter(ColumnDefinition::isExcludeFromSelect)
////            .map(ColumnDefinition::getName)
////            .collect(Collectors.toList());
//
//    return Stream.concat(
//            Arrays.stream(
//                    ctx.getFilesQuery()
//                        ? this.dataSetInfo
//                            .getTableInfo(RdbmsSchema.FILE_TABLE)
//                            .getColumnDefinitions()
//                        : this.entityTable.getColumnDefinitions())
////                .filter(
////                    definition ->
////                        !(ctx.getFilesQuery() && fileFilteredFields.contains(definition.getName()))
////                            && (skipExcludes || !filteredFields.contains(definition.getName())))
//                .map(
//                    definition -> {
//                      String fieldSelect = String.format("%s.%s", prefix, definition.getName());
//                      return String.format("%1$s AS %2$s", fieldSelect, definition.getName());
//                    }),
//            idSelects)
//        .distinct();
//  }

  protected String getWithStatement() {
    return String.format(
        "WITH %s ",
        this.viewListBuilder.build().stream()
            .map(View::toString)
            .collect(Collectors.joining(", ")));
  }
}
