package bio.terra.cda.app.models;

import bio.terra.cda.app.builders.ColumnsReturnBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/*
* DataSetInfo
*
* This class contains information about the various schemas that exist in the CDA database. There are two maps that are
* of great importance for fieldMap and tableInfoMap. The fieldMap contains a mapping between field names and the data
* around those fields, aka what table they belong to, their data type and mode. The tableInfoMap is used to contain a
* mapping between table names and their corresponding TableInfo objects. The TableInfo class, in turn, contains schema
* information for the table as well as relationships to other TableInfo objects, building a relationship graph that can
* be traversed to allow for the query generators to apply the correct unnests and joins while writing out a query.
*
* */
public class DataSetInfo {
  private final Map<String, TableInfo> entityTableInfoMap;

  private final Map<String, TableInfo> mappingTableInfoMap;

  private final Map<String, ColumnDefinition> fieldMap;

  private final Map<String, String> knownAliases;

  private final Set<String> replacedFieldnames;

  private DataSetInfo(
      Map<String, TableInfo> entityTableInfoMap,
      Map<String, TableInfo> mappingTableInfoMap,
      Map<String, ColumnDefinition> fieldMap,  //don't add FKs
      Set<String> replacedFieldnames,
      Map<String, String> knownAliases) {
    this.entityTableInfoMap = entityTableInfoMap;
    this.mappingTableInfoMap = mappingTableInfoMap;
    this.fieldMap = fieldMap;
    this.replacedFieldnames = replacedFieldnames;
    this.knownAliases = knownAliases;
  }

  public TableInfo getTableInfo(String tableName) {
    TableInfo tableinfo = null;
    if (entityTableInfoMap.containsKey(tableName)) {
      tableinfo = getEntityTableInfo(tableName);
    } else {
      tableinfo = getMappingTableInfo(tableName);
    }
    return tableinfo;
  }

  public TableInfo getEntityTableInfo(String tableName) {
    return this.entityTableInfoMap.get(this.knownAliases.getOrDefault(tableName, tableName));
  }

  public TableInfo getMappingTableInfo(String tableName) {
    return this.mappingTableInfoMap.get(this.knownAliases.getOrDefault(tableName, tableName));
  }

  public static String getNewFieldNameForDuplicate(String name, String tableName) {
    return String.format("%s_%s", tableName, name);
  }

  public List<ColumnsReturn> getColumnsData() {
    return this.fieldMap.entrySet().stream()
        .map(
            entry ->
                ColumnsReturnBuilder.of(
                    entry.getValue().getTableName(),
                    entry.getKey(),
                    "", //entry.getValue().getDescription(),
                    entry.getValue().getType(),
                    entry.getValue().isNullable()))
        .collect(Collectors.toList());
  }

  public Map<String, String> getKnownAliases() {
    return knownAliases;
  }

  public ColumnDefinition getColumnDefinitionByFieldName(String fieldName) {
    return this.fieldMap.get(fieldName);
  }

  public ColumnDefinition getColumnDefinitionByFieldName(String fieldName, String tablename) {
    if (fieldName.contains(".")) {
      // it's a mapping field
      String[] parsed = fieldName.split("\\.", 2);
      TableInfo mappingTable = mappingTableInfoMap.get(parsed[0]);
      return Arrays.stream(mappingTable.getColumnDefinitions()).filter(col -> col.getName().equals(parsed[1])).findFirst().orElse(null);
    }
    return replacedFieldnames.contains(fieldName)
        ? getColumnDefinitionByFieldName(getNewFieldNameForDuplicate(fieldName, tablename))
        : getColumnDefinitionByFieldName(fieldName);

  }

  public TableInfo getTableInfoFromField(String fieldName) {
    ColumnDefinition col = this.fieldMap.get(fieldName);
    if (Objects.isNull(col)) {
      return null;
    }
    return this.getTableInfo(col.getTableName());
  }


  /*
   * DataSetInfoBuilder
   *
   * This is a builder class for creating the DataSetInfo object, used to contains information about
   * the data schema. This builder will take a table schema and process each of the fields in that schema.
   * If the field has a relationship specified to another table, the addTableSchema function is then recursively called
   * for that table.
   *
   * Every repeated record field in the schema is also listed as a TableInfo object, with the type of NESTED. Each NESTED
   * TableInfo object will have a relationship added to their parent TableInfo, with the TableRelationship including a flag
   * for parent = true;
   *
   * For fields that are found in the schema that have the same name, but under a different TableInfo, this builder will adjust
   * the name so that it is prefixed with the name of the TableInfo it belongs to. This is so that all of the fields
   * that exist in the fieldMap are completely unique.
   *
   * */
  public static class DataSetInfoBuilder {
    private final Map<String, TableInfo> entityTableInfoMap;

    private final Map<String, TableInfo> mappingTableInfoMap;
    private final Map<String, ColumnDefinition> fieldMap;

    private final Map<String, ColumnDefinition> internalFieldsMap;
    private final Set<String> usedFields;
    private final Map<String, String> knownAliases;

    public DataSetInfoBuilder() {
      this.entityTableInfoMap = new HashMap<>();
      this.mappingTableInfoMap = new HashMap<>();
      this.fieldMap = new ConcurrentHashMap<>();
      this.internalFieldsMap = new ConcurrentHashMap<>();
      this.usedFields = new HashSet<>();
      this.knownAliases = new HashMap<>();
    }

    public DataSetInfoBuilder setDbSchema(JsonNode dbSchema) {
      if (dbSchema.isArray()) {
        for (JsonNode node : dbSchema) {
          if (node.has("table_name")) {
            addTableFromJson(node);
          }
        }
      }
      return this;
    }

    private DataSetInfoBuilder addTableFromJson(JsonNode tableNode) {
      this.addTableFromJson(tableNode.get("table_name").asText(), tableNode);
      return this;
    }

    public DataSetInfo build() {
      connectForeignKeys();
//      entityTableInfoMap.putAll(mappingTableInfoMap);
      // get rid of the synchronized field map because after this point it should be read only. so also make it unmodifyable
      return new DataSetInfo(
          entityTableInfoMap,
          mappingTableInfoMap,
          Collections.unmodifiableMap(new HashMap<>(fieldMap)),
          usedFields,
          knownAliases);
    }

    private void connectForeignKeys() {
      mappingTableInfoMap.values().forEach(tableInfo -> {
        tableInfo.getRelationships().forEach(rel -> {
          addTableRelationships(rel);
        });
      });
    }

    private TableInfo getTableInfo(String key) {
      return entityTableInfoMap.containsKey(key)
          ? entityTableInfoMap.get(key)
          : mappingTableInfoMap.get(key);
    }

    private void addTableRelationships(TableRelationship rel) {
      TableInfo fromTable = this.getTableInfo(rel.getFromTablename());
      fromTable.addForeignKey(ForeignKey.ofSingle(fromTable.getTableName(), rel.getFromField(), rel.getDestinationTablename(), rel.getDestinationField()));

      TableInfo destTable = this.getTableInfo(rel.getDestinationTablename());
      destTable.addForeignKey((ForeignKey.ofSingle(destTable.getTableName(), rel.getDestinationField(), rel.getFromTablename(), rel.getFromField())));
    }

//    private void addForeignKey(TableRelationship rel) {
//      // Find from col Definition and add a FK based on the relationship
//      String fromField = rel.getFromField();
//      String fqFieldName = usedFields.contains(fromField) ? getNewFieldNameForDuplicate(fromField, rel.getFromTablename()) : fromField;
//      ColumnDefinition colDef = fieldMap.get(fqFieldName);
//      colDef.addForeignKey(ForeignKey.ofSingle(rel.getDestinationTablename(), rel.getDestinationField()));
//    }


    private void addTableFromJson(String tableName, JsonNode tableNode) {
      boolean isMappingTable = false;
      TableInfo.TableInfoBuilder builder =
          new TableInfo.TableInfoBuilder()
              .setTableName(tableName)
              .setColumnDefinitions(createColumnDefinitions(tableNode.get("columns"), tableName))
              .setPrimaryKeys(getPrimaryKeysFromJson(tableNode.get("alter").get("primary_keys")));
      if (tableNode.get("alter").has("columns")) {
        isMappingTable = true;
        builder.setIsMappingTable(true);
        builder.setTableRelationships(
            getRelationshipsFromJson(tableName, tableNode.get("alter").get("columns")));
      }
      TableInfo tableInfo = builder.build();
      addFieldsFromTable(tableInfo);
      // skip partition by
      if (isMappingTable) {
        this.mappingTableInfoMap.put(tableName, tableInfo);
      } else {
        this.entityTableInfoMap.put(tableName, tableInfo);
      }
    }

    private void addFieldsFromTable(TableInfo table) {
      String tableName = table.getTableName();
      ColumnDefinition[] cols = table.getColumnDefinitions();
      List<String>  fromFields = table.getRelationships().stream().map(TableRelationship::getFromField).collect(Collectors.toList());
      // divide fields into those that are only foreign keys to entity tables and then the rest
      Arrays.stream(cols)
          // skip fields that are just foreign keys to entity tables
          .filter(field -> !(table.getRelationships().stream().map(rel -> rel.getFromField()).collect(Collectors.toList())).contains(field.getName()))
          .forEach( col -> addFieldMapEntry(col, tableName));

//          Map<Boolean, List<ColumnDefinition>> areMappingFields = Arrays.stream(cols)
//          .collect(Collectors.partitioningBy(col -> fromFields.contains(col.getName())));
//      areMappingFields.get(Boolean.TRUE).forEach( col -> addFieldMapEntry(col, tableName,  internalFieldsMap));
//      areMappingFields.get(Boolean.FALSE).forEach( col -> addFieldMapEntry(col, tableName,  fieldMap));
    }

    private void addFieldMapEntry(ColumnDefinition colDef, String tableName) {
      String fieldName = colDef.getName();
      if (fieldMap.containsKey(fieldName) || usedFields.contains(fieldName)) {
          String alias = getNewFieldNameForDuplicate(fieldName, tableName);
          resolveFieldNameConflict(fieldName);
        colDef.setAlias(alias);
        fieldName = alias;
      }
      fieldMap.put(fieldName, colDef);
    }

    public void resolveFieldNameConflict(String name) {
      if (this.fieldMap.containsKey(name)) {
        usedFields.add(name);
        ColumnDefinition col = this.fieldMap.get(name);
        String alias = getNewFieldNameForDuplicate(name, col.getTableName());
        this.fieldMap.remove(name);
        this.fieldMap.put(alias, col);
        col.setAlias(alias);
      }
    }

    private List<TableRelationship> getRelationshipsFromJson(String fromTable, JsonNode fkNodeList) {
      return StreamSupport.stream(fkNodeList.spliterator(), false)
              .map(fkNode -> TableRelationship.of(
                  fkNode.get("constraint_name").asText(),
                      fromTable,
                      fkNode.get("name").asText(),
                      fkNode.get("references").get("table").asText(),
                      fkNode.get("references").get("column").asText()))
              .collect(Collectors.toList());
    }


    private List<String> getPrimaryKeysFromJson(JsonNode pkNodeList) {
      return StreamSupport.stream(pkNodeList.get(0).get("columns").spliterator(), false)
              .map(JsonNode::textValue)
              .collect(Collectors.toList());
    }


    private List<ColumnDefinition> createColumnDefinitions(JsonNode columnsNode, String tableName) {
      return StreamSupport.stream(columnsNode.spliterator(), false)
              .map(colNode -> createColumnDefinition(colNode, tableName))
              .collect(Collectors.toList());

      }

    private ColumnDefinition createColumnDefinition(JsonNode colNode, String tableName) {
      ColumnDefinition col = new ColumnDefinition(
              colNode.get("name").asText(),
              tableName,
              colNode.get("type").asText(),
              "", // TODO get comment
              colNode.get("nullable").asBoolean());
      return col;
    }


    private void setCountByTableInfo(ColumnDefinition definition, TableInfo tableInfo) {
      CountByField[] countByFields = definition.getCountByFields();
      if (Objects.nonNull(countByFields) && countByFields.length > 0) {
        for (CountByField countByField : countByFields) {
          if (Objects.isNull(countByField.getTable())) {
            countByField.setTableInfo(tableInfo);
          } else {
            TableInfo countByTableInfo =
                this.entityTableInfoMap.get(
                    this.knownAliases.getOrDefault(
                        countByField.getTable(), countByField.getTable()));

            countByField.setTableInfo(countByTableInfo);
          }
        }
      }
    }
  }
}
