package bio.terra.cda.app.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryContext {
  private final String table;
  private final String tableOrSubClause;
  private final String project;
  private List<Unnest> unnests;
  private List<String> select;
  private List<String> partitions;
  private final String fileTable;
  private EntitySchema entitySchema;
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

  public QueryContext setEntitySchema(EntitySchema schema) {
    this.entitySchema = schema;
    return this;
  }

  public EntitySchema getEntitySchema() {
    return this.entitySchema;
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

  public QueryContext addUnnests(Stream<Unnest> newUnnests) {
    var aliasIndexes = new HashMap<String, Integer>();

    newUnnests.forEach(unnest -> {
      Integer index = 0;
      boolean add = true;

      if (aliasIndexes.containsKey(unnest.getAlias())) {
        index = aliasIndexes.get(unnest.getAlias());

        // inner joins take precedence over all other join types
        this.unnests.set(
                index,
                this.unnests.get(index).getJoinType().equals(SqlUtil.JoinType.INNER)
                        ? this.unnests.get(index) : unnest);
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

  public List<Unnest> getUnnests() {
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

  public Map<String, String> getAliasMap() {
    return this.aliasMap;
  }

  public Boolean entityFound() {
    return this.entitySchema.wasFound();
  }

  public String getEntityPath() {
    return this.entitySchema.getPath();
  }

  public String[] getEntityParts() {
    return this.entitySchema.getParts();
  }

  public Stream<String> getEntityPartsStream() {
    return this.entitySchema.getPartsStream();
  }

  public String getEntityPrefix() {
    return this.entitySchema.getPrefix();
  }

  public TableSchema.SchemaDefinition[] getEntitySchemaFields() {
    return this.entitySchema.getSchemaFields();
  }
}
