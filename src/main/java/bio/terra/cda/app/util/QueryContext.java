package bio.terra.cda.app.util;

import bio.terra.cda.app.builders.*;
import bio.terra.cda.app.models.*;
import bio.terra.cda.generated.model.DatasetInfo;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryContext {
  private String table;

  private String subQueryTable;
  private boolean subQuery = false;
  private List<Select> select;
  private List<OrderBy> orderBys;
  private List<ColumnDefinition> groupBys;
  private Boolean includeSelect;
  private Boolean filesQuery;
  private QueryFieldBuilder queryFieldBuilder;
  private SelectBuilder selectBuilder;
  private JoinBuilder joinBuilder;
  private ParameterBuilder parameterBuilder;
  private OrderByBuilder orderByBuilder;
  private ViewListBuilder<? extends View, ? extends ViewBuilder> viewListBuilder;
  private TableInfo tableInfo;

  private TableInfo subQueryTableInfo;

  private LinkedHashMap<String, Join> joins;

  final DataSetInfo dataSetInfo;

  public QueryContext(String table) {
    this.table = table;

    this.select = new ArrayList<>();
    this.groupBys = new ArrayList<>();
    this.orderBys = new ArrayList<>();
    this.dataSetInfo = RdbmsSchema.getDataSetInfo();
  }

  public QueryContext setFilesQuery(boolean value) {
    filesQuery = value;
    return this;
  }

  public boolean getFilesQuery() {
    return filesQuery;
  }

  public QueryContext setTable(String table) {
    this.table = table;
    this.setTableInfo(dataSetInfo.getTableInfo(table));
    return this;
  }

  public QueryContext setTableInfo(TableInfo tableInfo) {
    this.tableInfo = tableInfo;
    return this;
  }

  public TableInfo getTableInfo() {
    return this.tableInfo;
  }

  public QueryContext setIncludeSelect(Boolean value) {
    this.includeSelect = value;
    return this;
  }

  public QueryContext setSubQueryTable(String subQueryTable) {
    this.subQueryTable = subQueryTable;
    this.setSubQueryTableInfo(dataSetInfo.getTableInfo(subQueryTable));
    return this;
  }

  public QueryContext setSubQueryTableInfo(TableInfo subQueryTableInfo) {
    this.subQueryTableInfo = subQueryTableInfo;
    return this;
  }

  public TableInfo getSubQueryTableInfo() {
    return this.subQueryTableInfo;
  }

  public QueryContext setSubQuery(boolean subQuery) {
    this.subQuery = subQuery;
    return this;
  }

  public QueryContext clearSubQuery() {
    this.subQueryTable = null;
    this.subQuery = false;
    return this;
  }

  public boolean isSubQuery() {
    return subQuery;
  }

  public Boolean getIncludeSelect() {
    return this.includeSelect;
  }

  public QueryContext setJoins(LinkedHashMap<String, Join> joins) {
    this.joins = joins;
    return this;
  }

  public QueryContext addJoins(List<Join> joinPath) {
    joinPath.forEach(
        join -> {
          String key = String.format("%s.%s", join.getKey().getFromTableName(), join.getKey().getDestinationTableName());
          Join matchingJoin = joins.get(key);
          if (matchingJoin != null) {
            // if we are already joining on these 2 tables either replace with an INNER join or leave as is
            if (join.getJoinType() == SqlUtil.JoinType.INNER && matchingJoin.getJoinType() != SqlUtil.JoinType.INNER) {
              matchingJoin.setJoinType(SqlUtil.JoinType.INNER);
            }
          } else {
            // otherwise add join
            joins.put(key, join);
          }
          });
    return this;
  }

  public QueryContext addSelects(Stream<Select> selects) {
    List<Select> newSelectsList = selects.collect(Collectors.toList());
    if (newSelectsList.isEmpty()) {
      return this;
    }

    this.select.addAll(newSelectsList);
    return this;
  }

  public QueryContext addGroupBy(ColumnDefinition col) {
    if (!groupBys.contains(col)) {
      groupBys.add(col);
    }
    return this;
  }

  public void addOrderBysToGroupBys() {
    orderBys.forEach(orderBy -> addGroupBy(orderBy.getColumnDefinition()));
  }

  public QueryContext addOrderBys(Stream<OrderBy> orderByStream) {
    List<OrderBy> newOrderByList = orderByStream.collect(Collectors.toList());
    if (newOrderByList.isEmpty()) {
      return this;
    }

    this.orderBys.addAll(newOrderByList);
    return this;
  }

  public List<Join> getJoins() {
    return this.joins.values().stream().collect(Collectors.toList());
  }

  public String getTable() {
    return this.table;
  }

  public List<Select> getSelect() {
    return this.select;
  }

  public List<OrderBy> getOrderBys() {
    return orderBys;
  }

  public List<ColumnDefinition> getGroupBys() {
    return groupBys;
  }
  public QueryFieldBuilder getQueryFieldBuilder() {
    return queryFieldBuilder;
  }

  public QueryContext setQueryFieldBuilder(QueryFieldBuilder queryFieldBuilder) {
    this.queryFieldBuilder = queryFieldBuilder;
    return this;
  }

  public SelectBuilder getSelectBuilder() {
    return selectBuilder;
  }

  public QueryContext setSelectBuilder(SelectBuilder selectBuilder) {
    this.selectBuilder = selectBuilder;
    return this;
  }

  public JoinBuilder getJoinBuilder() {
    return joinBuilder;
  }

  public QueryContext setJoinBuilder(JoinBuilder joinBuilder) {
    this.joinBuilder = joinBuilder;
    return this;
  }

  public ParameterBuilder getParameterBuilder() {
    return parameterBuilder;
  }

  public QueryContext setParameterBuilder(ParameterBuilder parameterBuilder) {
    this.parameterBuilder = parameterBuilder;
    return this;
  }

  public OrderByBuilder getOrderByBuilder() {
    return orderByBuilder;
  }

  public QueryContext setOrderByBuilder(OrderByBuilder orderByBuilder) {
    this.orderByBuilder = orderByBuilder;
    return this;
  }

  public ViewListBuilder<? extends View, ? extends ViewBuilder> getViewListBuilder() {
    return viewListBuilder;
  }

  public QueryContext setViewListBuilder(ViewListBuilder<? extends View, ? extends ViewBuilder> viewListBuilder) {
    this.viewListBuilder = viewListBuilder;
    return this;
  }
}
