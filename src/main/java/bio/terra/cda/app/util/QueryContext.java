package bio.terra.cda.app.util;

import bio.terra.cda.app.builders.ParameterBuilder;
import bio.terra.cda.app.builders.PartitionBuilder;
import bio.terra.cda.app.builders.QueryFieldBuilder;
import bio.terra.cda.app.builders.SelectBuilder;
import bio.terra.cda.app.builders.UnnestBuilder;
import bio.terra.cda.app.models.Partition;
import bio.terra.cda.app.models.Select;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.models.Unnest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryContext {
  private final String table;
  private final String project;
  private List<Unnest> unnests;
  private List<Select> select;
  private List<Partition> partitions;
  private Boolean includeSelect;
  private Boolean filesQuery;
  private QueryFieldBuilder queryFieldBuilder;
  private SelectBuilder selectBuilder;
  private UnnestBuilder unnestBuilder;
  private PartitionBuilder partitionBuilder;
  private ParameterBuilder parameterBuilder;
  private TableInfo tableInfo;

  public QueryContext(String table, String project) {
    this.table = table;
    this.project = project;

    this.unnests = new ArrayList<>();
    this.select = new ArrayList<>();
    this.partitions = new ArrayList<>();
  }

  public QueryContext setFilesQuery(boolean value) {
    filesQuery = value;
    return this;
  }

  public boolean getFilesQuery() {
    return filesQuery;
  }

  public String getProject() {
    return this.project;
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

  public QueryFieldBuilder getQueryFieldBuilder() {
    return this.queryFieldBuilder;
  }

  public QueryContext setQueryFieldBuilder(QueryFieldBuilder builder) {
    this.queryFieldBuilder = builder;
    return this;
  }

  public SelectBuilder getSelectBuilder() {
    return this.selectBuilder;
  }

  public QueryContext setSelectBuilder(SelectBuilder builder) {
    this.selectBuilder = builder;
    return this;
  }

  public UnnestBuilder getUnnestBuilder() {
    return this.unnestBuilder;
  }

  public QueryContext setUnnestBuilder(UnnestBuilder builder) {
    this.unnestBuilder = builder;
    return this;
  }

  public PartitionBuilder getPartitionBuilder() {
    return this.partitionBuilder;
  }

  public QueryContext setPartitionBuilder(PartitionBuilder builder) {
    this.partitionBuilder = builder;
    return this;
  }

  public ParameterBuilder getParameterBuilder() {
    return this.parameterBuilder;
  }

  public QueryContext setParameterBuilder(ParameterBuilder builder) {
    this.parameterBuilder = builder;
    return this;
  }

  public QueryContext addUnnests(Stream<Unnest> newUnnests) {
    var aliasIndexes = new HashMap<String, Integer>();

    newUnnests.forEach(
        unnest -> {
          Integer index = 0;
          boolean add = true;

          if (aliasIndexes.containsKey(unnest.getAlias())) {
            index = aliasIndexes.get(unnest.getAlias());

            // inner joins take precedence over all other join types
            this.unnests.set(
                index,
                this.unnests.get(index).getJoinType().equals(SqlUtil.JoinType.INNER)
                    ? this.unnests.get(index)
                    : unnest);
          } else {
            for (var current : this.unnests) {
              aliasIndexes.put(current.getAlias(), index);

              if (current.getAlias().equals(unnest.getAlias())) {
                if (current.getJoinType().equals(SqlUtil.JoinType.INNER)) {
                  add = false;
                }

                break;
              }

              index++;
            }

            if (add) {
              if (index.equals(this.unnests.size())) {
                this.unnests.add(unnest);
              } else {
                this.unnests.set(index, unnest);
              }
            }
          }
        });

    return this;
  }

  public QueryContext addPartitions(Stream<Partition> newPartitions) {
    this.partitions.addAll(newPartitions.collect(Collectors.toList()));
    return this;
  }

  public QueryContext addSelects(Stream<Select> selects) {
    this.select.addAll(selects.collect(Collectors.toList()));
    return this;
  }

  public String getTable() {
    return this.table;
  }

  public List<Select> getSelect() {
    return this.select;
  }

  public List<Unnest> getUnnests() {
    return this.unnests;
  }

  public List<Partition> getPartitions() {
    return partitions;
  }
}
