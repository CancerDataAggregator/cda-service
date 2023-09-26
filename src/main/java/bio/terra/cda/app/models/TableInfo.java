package bio.terra.cda.app.models;

import bio.terra.cda.app.service.TablePrecedenceComparator;

import java.util.*;
import java.util.stream.Collectors;

public class TableInfo {
  private final String tableName;
  private final ColumnDefinition[] columnDefinitions;
  private final List<TableRelationship> relationships;
  private final List<ColumnDefinition> primaryKeys;

  private SortedSet<ForeignKey> foreignKeys;

  boolean isMappingTable = false;

  private TableInfo(
      String tableName,
      ColumnDefinition[] columnDefinitions,
      List<ColumnDefinition> primaryKeys,
      List<TableRelationship> relationships,
      boolean isMappingTable
      ) {
    this(tableName, columnDefinitions, primaryKeys, relationships);
    this.isMappingTable = isMappingTable;
    this.foreignKeys = new TreeSet<>(new TablePrecedenceComparator());
  }

  private TableInfo(
      String tableName,
      ColumnDefinition[] columnDefinitions,
      List<ColumnDefinition> primaryKeys,
      List<TableRelationship> relationships) {
    this.tableName = tableName;
    this.columnDefinitions = columnDefinitions;
    this.primaryKeys = primaryKeys;
    this.relationships = relationships;
  }

  public void addForeignKey(ForeignKey foreignKey) {
    this.foreignKeys.add(foreignKey);
  }

  public String getTableName() {
    return tableName;
  }

  public boolean isMapppingTable() {
    return this.isMappingTable;
  }



  public List<TableRelationship> getRelationships() {
    return this.relationships == null ? Collections.emptyList() : this.relationships;
  }

  public SortedSet<ForeignKey> getForeignKeys() {
    return this.foreignKeys == null ? Collections.emptySortedSet() : this.foreignKeys;
  }


  public ColumnDefinition[] getColumnDefinitions() {
    return columnDefinitions;
  }


  public String getTableAlias(DataSetInfo dataSetInfo) {
      return dataSetInfo.getKnownAliases().getOrDefault(this.tableName, this.tableName);
  }

  public List<ColumnDefinition> getPrimaryKeys() {
    return this.primaryKeys == null ? Collections.emptyList() : this.primaryKeys;
  }

  public List<String> getPrimaryKeysAlias() {
    return this.primaryKeys.stream().map(ColumnDefinition::getAlias).collect(Collectors.toList());
  }


  public static class TableInfoBuilder {
    private String tableName;
    private boolean isMappingTable = false;
    private ColumnDefinition[] columnDefinitions;
    private List<String> primaryKeys;

    private List<TableRelationship> relationships;

    private List<ForeignKey> foreignKeys;

  public TableInfoBuilder() {
    }

    public TableInfoBuilder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public TableInfoBuilder setColumnDefinitions(
        List<ColumnDefinition> columnDefinitions) {
      this.columnDefinitions = columnDefinitions.toArray(ColumnDefinition[]::new);
      return this;
    }

    public TableInfoBuilder setPrimaryKeys(List<String> primaryKeys) {
      this.primaryKeys = primaryKeys;
      return this;
    }

    public TableInfoBuilder setTableRelationships(List<TableRelationship> relationships) {
      this.relationships = relationships;
      return this;
    }

    public TableInfoBuilder setIsMappingTable(boolean isMappingTable) {
      this.isMappingTable = isMappingTable;
      return this;
    }


    public TableInfo build() {
      return new TableInfo(
          this.tableName,
          this.columnDefinitions,
          getColumnDefinitionForPrimaryKeys(),
          this.relationships,
          this.isMappingTable);
    }

    protected List<ColumnDefinition> getColumnDefinitionForPrimaryKeys() {
      return Arrays.stream(columnDefinitions)
          .filter(cd -> primaryKeys.contains(cd.getName())).collect(Collectors.toList());
    }

  }
}
