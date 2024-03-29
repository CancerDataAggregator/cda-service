package bio.terra.cda.app.models;

import bio.terra.cda.app.util.TableSchema;
import com.google.cloud.Tuple;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class TableInfo {
  private final String tableName;
  private String adjustedTableName;
  private final TableSchema.SchemaDefinition[] schemaDefinitions;
  private final List<TableRelationship> relationships;
  private final TableInfoTypeEnum type;
  private final String partitionKey;

  private TableInfo(
      String tableName,
      String adjustedTableName,
      TableInfoTypeEnum type,
      TableSchema.SchemaDefinition[] schemaDefinitions,
      String partitionKey) {
    this.tableName = tableName;
    this.adjustedTableName = adjustedTableName;
    this.partitionKey = partitionKey;
    this.relationships = new ArrayList<>();
    this.type = type;
    this.schemaDefinitions = schemaDefinitions;
  }

  public String getTableName() {
    return tableName;
  }

  public String getAdjustedTableName() {
    return adjustedTableName;
  }

  public void setAdjustedTableName(String adjustedTableName) {
    this.adjustedTableName = adjustedTableName;
  }

  public void addRelationship(TableRelationship tableRelationship) {
    Optional<TableRelationship> optRelationship =
        relationships.stream()
            .filter(
                rel ->
                    rel.getDestinationTableInfo()
                            .getTableName()
                            .equals(tableRelationship.getDestinationTableInfo().getTableName())
                        && rel.getField().equals(tableRelationship.getField()))
            .findFirst();

    if (optRelationship.isPresent()) {
      var relationship = optRelationship.get();
      for (ForeignKey foreignKey : tableRelationship.getForeignKeys()) {
        relationship.addForeignKey(foreignKey);
      }
    } else {
      this.relationships.add(tableRelationship);
    }
  }

  public List<TableRelationship> getRelationships() {
    return this.relationships;
  }

  public TableInfoTypeEnum getType() {
    return type;
  }

  public TableSchema.SchemaDefinition[] getSchemaDefinitions() {
    return schemaDefinitions;
  }

  public String getPartitionKeyFullName(DataSetInfo dataSetInfo) {
    if (Objects.nonNull(dataSetInfo.getSchemaDefinitionByFieldName(this.partitionKey))) {
      return this.partitionKey;
    }

    String table = this.adjustedTableName.toLowerCase(Locale.ROOT);

    if (this.getType().equals(TableInfoTypeEnum.TABLE)) {
      table =
          dataSetInfo
              .getKnownAliases()
              .getOrDefault(this.tableName, this.tableName)
              .toLowerCase(Locale.ROOT);
    }

    if (getType().equals(TableInfoTypeEnum.ARRAY)) {
      return table;
    }

    return String.format("%s_%s", table, this.partitionKey);
  }

  public String getTableAlias(DataSetInfo dataSetInfo) {
    if (this.getType().equals(TableInfoTypeEnum.TABLE)) {
      return dataSetInfo.getKnownAliases().getOrDefault(this.tableName, this.tableName);
    } else {
      return String.format("_%s", this.adjustedTableName);
    }
  }

  public TableInfo getSuperTableInfo() {
    if (this.getType().equals(TableInfoTypeEnum.TABLE)) {
      return this;
    } else {
      TableInfo current = this;

      while (!current.getType().equals(TableInfoTypeEnum.TABLE)) {
        TableRelationship parent =
            current.getRelationships().stream()
                .filter(TableRelationship::isParent)
                .findFirst()
                .orElseThrow();

        current = parent.getDestinationTableInfo();
      }

      return current;
    }
  }

  public String getPartitionKey() {
    return partitionKey;
  }

  public String getPartitionKeyAlias(DataSetInfo dataSetInfo) {
    if (getType().equals(TableInfoTypeEnum.ARRAY)) {
      return this.getTableAlias(dataSetInfo);
    }

    return String.format("%s.%s", this.getTableAlias(dataSetInfo), this.partitionKey);
  }

  public TableRelationship[] getTablePath() {
    if (this.getType().equals(TableInfoTypeEnum.TABLE)) {
      return new TableRelationship[0];
    }

    return getTablePath(this, this);
  }

  private static TableRelationship[] getTablePath(TableInfo current, TableInfo target) {
    TableRelationship parent =
        current.getRelationships().stream()
            .filter(TableRelationship::isParent)
            .findFirst()
            .orElseThrow();

    if (parent.getDestinationTableInfo().getType().equals(TableInfoTypeEnum.TABLE)) {
      return parent.getDestinationTableInfo().getPathToTable(target);
    } else {
      return getTablePath(parent.getDestinationTableInfo(), target);
    }
  }

  public TableRelationship[] getPathToTable(TableInfo tableInfo) {
    return this.getPathToTable(tableInfo, false);
  }

  public TableRelationship[] getPathToTable(TableInfo tableInfo, boolean noParent) {
    if (tableInfo.getAdjustedTableName().equals(getAdjustedTableName())) {
      return new TableRelationship[0];
    }

    LinkedList<Tuple<TableRelationship[], TableRelationship>> queue =
        this.getRelationships().stream()
            .filter(tableRelationship -> (!tableRelationship.isParent() || !noParent))
            .map(tableRelationship -> Tuple.of(new TableRelationship[0], tableRelationship))
            .collect(Collectors.toCollection(LinkedList::new));
    Map<String, Boolean> visited =
        new HashMap<>() {
          {
            put(getTableName(), true);
          }
        };

    while (queue.size() > 0) {
      Tuple<TableRelationship[], TableRelationship> tuple = queue.pop();
      TableRelationship[] currentPath = tuple.x();
      TableRelationship tableRelationship = tuple.y();
      var foreignKey = tableRelationship.getForeignKeys();
      if (foreignKey.stream()
          .anyMatch(fk -> Objects.nonNull(fk.getLocation()) && fk.getLocation().length() > 0)) {
        continue;
      }

      List<TableRelationship> relList = new ArrayList<>(List.of(currentPath));
      relList.add(tableRelationship);
      TableRelationship[] relArray = relList.toArray(TableRelationship[]::new);

      if (tableRelationship
          .getDestinationTableInfo()
          .getAdjustedTableName()
          .equals(tableInfo.getAdjustedTableName())) {
        return relArray;
      }

      TableInfo nextTable = tableRelationship.getDestinationTableInfo();
      visited.put(nextTable.getAdjustedTableName(), true);

      List<Tuple<TableRelationship[], TableRelationship>> moreRelationships =
          nextTable.getRelationships().stream()
              .filter(
                  tr ->
                      Objects.isNull(
                              visited.get(tr.getDestinationTableInfo().getAdjustedTableName()))
                          && (!tableRelationship.isParent() || !noParent))
              .map(tr -> Tuple.of(relArray, tr))
              .collect(Collectors.toList());

      if (!moreRelationships.isEmpty()) {
        queue.addAll(moreRelationships);
      }
    }

    return null;
  }

  public enum TableInfoTypeEnum {
    NESTED,
    TABLE,
    ARRAY
  }

  public static TableInfo of(String tableName, String partitionKey) {
    return new TableInfoBuilder().setTableName(tableName).setPartitionKey(partitionKey).build();
  }

  public static TableInfo of(
      String tableName,
      TableInfoTypeEnum type,
      TableSchema.SchemaDefinition[] schemaDefinitions,
      String partitionKey) {
    return new TableInfoBuilder()
        .setTableName(tableName)
        .setType(type)
        .setSchemaDefinitions(Arrays.asList(schemaDefinitions))
        .setPartitionKey(partitionKey)
        .build();
  }

  public static TableInfo of(
      String tableName,
      String adjustedTableName,
      TableInfoTypeEnum type,
      TableSchema.SchemaDefinition[] schemaDefinitions,
      String partitionKey) {
    return new TableInfoBuilder()
        .setTableName(tableName)
        .setAdjustedTableName(adjustedTableName)
        .setType(type)
        .setSchemaDefinitions(Arrays.asList(schemaDefinitions))
        .setPartitionKey(partitionKey)
        .build();
  }

  public static class TableInfoBuilder {
    private TableInfoTypeEnum type;
    private String tableName;
    private String adjustedTableName;
    private TableSchema.SchemaDefinition[] schemaDefinitions;
    private String partitionKey;

    public TableInfoBuilder() {
      this.type = TableInfoTypeEnum.TABLE;
    }

    public TableInfoBuilder setType(TableInfoTypeEnum type) {
      this.type = type;
      return this;
    }

    public TableInfoBuilder setTableName(String tableName) {
      this.tableName = tableName;

      if (Objects.isNull(this.adjustedTableName)) {
        this.adjustedTableName = tableName;
      }

      return this;
    }

    public TableInfoBuilder setSchemaDefinitions(
        List<TableSchema.SchemaDefinition> schemaDefinitions) {
      this.schemaDefinitions = schemaDefinitions.toArray(TableSchema.SchemaDefinition[]::new);
      return this;
    }

    public TableInfoBuilder setPartitionKey(String partitionKey) {
      this.partitionKey = partitionKey;
      return this;
    }

    public TableInfo build() {
      return new TableInfo(
          this.tableName,
          this.adjustedTableName,
          this.type,
          this.schemaDefinitions,
          this.partitionKey);
    }

    public TableInfoBuilder setAdjustedTableName(String adjustedTableName) {
      this.adjustedTableName = adjustedTableName;
      return this;
    }
  }
}
