package bio.terra.cda.app.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryContext {
  private final String table;
  private final String tableOrSubClause;
  private final String project;
  private List<String> unnests;
  private List<String> select;
  private List<String> partitions;
  private final String fileTable;
  private String entityPath;
  private final Map<String, TableSchema.SchemaDefinition> tableSchemaMap;
  private final Map<String, TableSchema.SchemaDefinition> fileTableSchemaMap;
  private Map<String, String> aliasMap;
  private Boolean includeSelect;
  private Boolean modularEntity;
  private Boolean filesQuery;

  public QueryContext(
      Map<String, TableSchema.SchemaDefinition> tableSchemaMap,
      String tableOrSubClause,
      String table,
      String project,
      String fileTable,
      Map<String, TableSchema.SchemaDefinition> fileTableSchemaMap) {
    this.tableOrSubClause = tableOrSubClause;
    this.table = table;
    this.project = project;
    this.tableSchemaMap = tableSchemaMap;
    this.fileTable = fileTable;
    this.fileTableSchemaMap = fileTableSchemaMap;

    this.unnests = new ArrayList<>();
    this.select = new ArrayList<>();
    this.partitions = new ArrayList<>();
    this.aliasMap = new HashMap<>();
  }

  public QueryContext setFilesQuery(Boolean value) {
    filesQuery = value;
    return this;
  }

  public Boolean getFilesQuery() {
    return filesQuery;
  }

  public String getProject() {
    return this.project;
  }

  public String getFileTable() {
    return this.fileTable;
  }

  public QueryContext setEntityPath(String value) {
    this.entityPath = value;
    return this;
  }

  public String getEntityPath() {
    return this.entityPath;
  }

  public QueryContext setModularEntity(Boolean value) {
    this.modularEntity = value;
    return this;
  }

  public Boolean getModularEntity() {
    return this.modularEntity;
  }

  public QueryContext setIncludeSelect(Boolean value) {
    this.includeSelect = value;
    return this;
  }

  public Boolean getIncludeSelect() {
    return this.includeSelect;
  }

  public QueryContext addUnnests(Stream<String> newUnnests) {
    this.unnests.addAll(newUnnests.collect(Collectors.toList()));
    return this;
  }

  public QueryContext addPartitions(Stream<String> newPartitions) {
    this.partitions.addAll(newPartitions.collect(Collectors.toList()));
    return this;
  }

  public QueryContext addSelect(String select) {
    this.select.add(select);
    return this;
  }

  public Map<String, TableSchema.SchemaDefinition> getTableSchemaMap() {
    return this.tableSchemaMap;
  }

  public Map<String, TableSchema.SchemaDefinition> getFileTableSchemaMap() {
    return this.fileTableSchemaMap;
  }

  public String getTable() {
    return this.table;
  }

  public List<String> getSelect() {
    return this.select;
  }

  public List<String> getUnnests() {
    return this.unnests;
  }

  public List<String> getPartitions() {
    return partitions;
  }

  public String getTableOrSubClause() {
    return this.tableOrSubClause;
  }

  public QueryContext addAlias(String key, String value) {
    this.aliasMap.put(key, value);
    return this;
  }

  public Map<String, String> getAliasMap() { return this.aliasMap; }
}
