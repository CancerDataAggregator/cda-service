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

//  public String getAdjustedTableName() {
//    return adjustedTableName;
//  }
//
//  public void setAdjustedTableName(String adjustedTableName) {
//    this.adjustedTableName = adjustedTableName;
//  }

//  public void addRelationship(TableRelationship tableRelationship) {
//    Optional<TableRelationship> optRelationship =
//        relationships.stream()
//            .filter(
//                rel ->
//                    rel.getDestinationTableInfo()
//                            .getTableName()
//                            .equals(tableRelationship.getDestinationTableInfo().getTableName())
//                        && rel.getField().equals(tableRelationship.getField()))
//            .findFirst();
//
//    if (optRelationship.isPresent()) {
//      var relationship = optRelationship.get();
//      for (ForeignKey foreignKey : tableRelationship.getForeignKeys()) {
//        relationship.addForeignKey(foreignKey);
//      }
//    } else {
//      this.relationships.add(tableRelationship);
//    }
//  }

  public List<TableRelationship> getRelationships() {
    return this.relationships == null ? Collections.emptyList() : this.relationships;
  }

  public SortedSet<ForeignKey> getForeignKeys() {
    return this.foreignKeys == null ? Collections.emptySortedSet() : this.foreignKeys;
  }


  public ColumnDefinition[] getColumnDefinitions() {
    return columnDefinitions;
  }

//  public List<String> getPrimaryKeysFullName(DataSetInfo dataSetInfo) {
//    return Arrays.stream(this.primaryKeys).map(dataSetInfo.getColumnDefinitionByFieldName).collect(Collectors.toList());
//    if (Objects.nonNull(dataSetInfo.getColumnDefinitionByFieldName(this.primaryKeys))) {
//      return this.partitionKey;
//    }
//
//    String table = this.adjustedTableName.toLowerCase(Locale.ROOT);
//
//      table =
//          dataSetInfo
//              .getKnownAliases()
//              .getOrDefault(this.tableName, this.tableName)
//              .toLowerCase(Locale.ROOT);
//
//    return String.format("%s_%s", table, this.partitionKey);
//  }

  public String getTableAlias(DataSetInfo dataSetInfo) {
      return dataSetInfo.getKnownAliases().getOrDefault(this.tableName, this.tableName);
  }

  public List<ColumnDefinition> getPrimaryKeys() {
    return this.primaryKeys == null ? Collections.emptyList() : this.primaryKeys;
  }

  public List<String> getPrimaryKeysAlias() {
    return this.primaryKeys.stream().map(ColumnDefinition::getAlias).collect(Collectors.toList());
  }

//  public String getPartitionKeyAlias(DataSetInfo dataSetInfo) {
//    if (getType().equals(TableInfoTypeEnum.ARRAY)) {
//      return this.getTableAlias(dataSetInfo);
//    }
//
//    return String.format("%s.%s", this.getTableAlias(dataSetInfo), this.partitionKey);
//  }


//  public TableRelationship[] getPathToTable(TableInfo tableInfo) {
//    if (tableInfo.getTableName().equals(getTableName())) {
//      return new TableRelationship[0];
//    }

//    DataSetInfo dataSetInfo = RdbmsSchema.getDataSetInfo();
//    LinkedList<Tuple<TableRelationship[], TableRelationship>> queue =
//        this.getRelationships().stream()
////            .filter(tableRelationship -> (!tableRelationship.isParent() || !noParent))
//            .map(tableRelationship -> Tuple.of(new TableRelationship[0], tableRelationship))
//            .collect(Collectors.toCollection(LinkedList::new));
//    Map<String, Boolean> visited =
//        new HashMap<>() {
//          {
//            put(getTableName(), true);
//          }
//        };
//
//    while (queue.size() > 0) {
//      Tuple<TableRelationship[], TableRelationship> tuple = queue.pop();
//      TableRelationship[] currentPath = tuple.x();
//      TableRelationship tableRelationship = tuple.y();
//      var foreignKey = tableRelationship.getForeignKeys();
//      if (foreignKey.stream()
//          .anyMatch(fk -> Objects.nonNull(fk.getLocation()) && fk.getLocation().length() > 0)) {
//        continue;
//      }
//
//      List<TableRelationship> relList = new ArrayList<>(List.of(currentPath));
//      relList.add(tableRelationship);
//      TableRelationship[] relArray = relList.toArray(TableRelationship[]::new);
//
//      if (tableRelationship
//          .getDestinationTable()
//          .equals(tableInfo.getTableName())) {
//        return relArray;
//      }
//
//      String nextTableName = tableRelationship.getDestinationTable();
//      visited.put(nextTableName, true);
//
//      TableInfo nextTable = dataSetInfo.getTableInfo(nextTableName);
//
//      List<Tuple<TableRelationship[], TableRelationship>> moreRelationships =
//          nextTable.getRelationships().stream()
//              .filter(
//                  tr ->
//                      Objects.isNull(
//                              visited.get(tr.getDestinationTable())))
////                          && (!tableRelationship.isParent() || !noParent))
//              .map(tr -> Tuple.of(relArray, tr))
//              .collect(Collectors.toList());
//
//      if (!moreRelationships.isEmpty()) {
//        queue.addAll(moreRelationships);
//      }
//    }

//    return null;
//  }

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

//    public TableInfoBuilder setAdjustedTableName(String adjustedTableName) {
//      this.adjustedTableName = adjustedTableName;
//      return this;
//    }
  }
}
