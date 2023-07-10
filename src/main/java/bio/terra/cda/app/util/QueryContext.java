package bio.terra.cda.app.util;

import bio.terra.cda.app.builders.*;
import bio.terra.cda.app.models.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryContext {
  private final String table;
  private List<Select> select;
  private List<Partition> partitions;
  private List<OrderBy> orderBys;
  private List<ColumnDefinition> groupBys;
  private Boolean includeSelect;
  private Boolean filesQuery;
  private QueryFieldBuilder queryFieldBuilder;
  private SelectBuilder selectBuilder;
  private JoinBuilder joinBuilder;
  private ParameterBuilder parameterBuilder;
  private OrderByBuilder orderByBuilder;
  private Optional<Integer> limit = Optional.empty();
  private Optional<Integer> offset = Optional.empty();
  private ViewListBuilder<? extends View, ? extends ViewBuilder> viewListBuilder;
  private TableInfo tableInfo;

  private List<List<Join>> joins;

  public QueryContext(String table) {
    this.table = table;

    this.select = new ArrayList<>();
    this.partitions = new ArrayList<>();
    this.groupBys = new ArrayList<>();
    this.orderBys = new ArrayList<>();
    this.joins = new ArrayList<>();
  }

  public QueryContext setFilesQuery(boolean value) {
    filesQuery = value;
    return this;
  }

  public boolean getFilesQuery() {
    return filesQuery;
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

  public Boolean getIncludeSelect() {
    return this.includeSelect;
  }

  public QueryContext addJoins(List<Join> joinPath) {
    this.joins.add(joinPath);
    return this;
  }

  // TODO incorporate this logic
  //  public QueryContext addUnnests(Stream<Unnest> newUnnests) {
  //    var aliasIndexes = new HashMap<String, Integer>();
  //
  //    newUnnests.forEach(
  //        unnest -> {
  //          Integer index = 0;
  //          boolean add = true;
  //
  //          if (aliasIndexes.containsKey(unnest.getAlias())) {
  //            index = aliasIndexes.get(unnest.getAlias());
  //
  //            // inner joins take precedence over all other join types
  //            this.unnests.set(
  //                index,
  //                this.unnests.get(index).getJoinType().equals(SqlUtil.JoinType.INNER)
  //                    ? this.unnests.get(index)
  //                    : unnest);
  //          } else {
  //            for (var current : this.unnests) {
  //              aliasIndexes.put(current.getAlias(), index);
  //
  //              if (current.getAlias().equals(unnest.getAlias())) {
  //                if (current.getJoinType().equals(SqlUtil.JoinType.INNER)) {
  //                  add = false;
  //                }
  //
  //                break;
  //              }
  //
  //              index++;
  //            }
  //
  //            if (add) {
  //              if (index.equals(this.unnests.size())) {
  //                this.unnests.add(unnest);
  //              } else {
  //                this.unnests.set(index, unnest);
  //              }
  //            }
  //          }
  //        });
  //
  //    return this;
  //  }

  public QueryContext addSelects(Stream<Select> selects) {
    List<Select> newSelectsList = selects.collect(Collectors.toList());
    if (newSelectsList.isEmpty()) {
      return this;
    }

    this.select.addAll(newSelectsList);
    return this;
  }

  public QueryContext addGroupBy(ColumnDefinition col) {
    groupBys.add(col);
    return this;
  }

  public QueryContext addOrderBys(Stream<OrderBy> orderByStream) {
    List<OrderBy> newOrderByList = orderByStream.collect(Collectors.toList());
    if (newOrderByList.isEmpty()) {
      return this;
    }

    this.orderBys.addAll(newOrderByList);
    return this;
  }

  public List<List<Join>> getJoins() {
    return this.joins;
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

  public QueryContext setViewListBuilder(
      ViewListBuilder<? extends View, ? extends ViewBuilder> viewListBuilder) {
    this.viewListBuilder = viewListBuilder;
    return this;
  }
}
