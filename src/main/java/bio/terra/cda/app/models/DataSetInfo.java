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

  private final Map<String, ColumnDefinition> entityTableFieldMap;
  private final Map<String, ColumnDefinition> mappingTableFieldMap;

  private final Map<String, String> knownAliases;

  private final Set<String> replacedEntityFieldnames;
  private final Set<String> replacedMappingFieldnames;

  private DataSetInfo(
      Map<String, TableInfo> entityTableInfoMap,
      Map<String, TableInfo> mappingTableInfoMap,
      Map<String, ColumnDefinition> entityTableFieldMap,
      Map<String, ColumnDefinition> mappingTableFieldMap,
      Set<String> replacedEntityFieldnames,
      Set<String> replacedMappingFieldnames,
      Map<String, String> knownAliases) {
    this.entityTableInfoMap = entityTableInfoMap;
    this.mappingTableInfoMap = mappingTableInfoMap;
    this.entityTableFieldMap = entityTableFieldMap;
    this.mappingTableFieldMap  = mappingTableFieldMap;
    this.replacedEntityFieldnames = replacedEntityFieldnames;
    this.replacedMappingFieldnames = replacedMappingFieldnames;
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
    return this.entityTableFieldMap.entrySet().stream()
        .map(
            entry ->
                ColumnsReturnBuilder.of(
                    entry.getValue().getEndpointName(),
                    entry.getKey(),
                    entry.getValue().getDescription(),
                    entry.getValue().getType(),
                    entry.getValue().isNullable()))
        .collect(Collectors.toList());
  }

  public Map<String, String> getKnownAliases() {
    return knownAliases;
  }

  public ColumnDefinition getColumnDefinitionByFieldName(String fieldName) {
    if (this.entityTableFieldMap.containsKey(fieldName)) {
      return this.entityTableFieldMap.get(fieldName);
    } else {
      return this.mappingTableFieldMap.get(fieldName);
    }
  }

  public ColumnDefinition getColumnDefinitionByFieldName(String fieldName, String tablename) {
    if (fieldName.contains(".")) {
      // it's a mapping field
      String[] parsed = fieldName.split("\\.", 2);
      String parsedTablename = parsed[0];
      String parsedColname = parsed[1];
      TableInfo mappingTable = getTableInfo(parsedTablename);
      return Arrays.stream(mappingTable.getColumnDefinitions())
          .filter(col -> col.getName().equals(parsedColname))
          .findFirst().orElse(null);
    }
    return replacedEntityFieldnames.contains(fieldName) || replacedMappingFieldnames.contains(fieldName)
        ? getColumnDefinitionByFieldName(getNewFieldNameForDuplicate(fieldName, tablename))
        : getColumnDefinitionByFieldName(fieldName);

  }

  public TableInfo getTableInfoFromField(String fieldName) {
    ColumnDefinition col = this.getColumnDefinitionByFieldName(fieldName);
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
    private final Map<String, ColumnDefinition> entityFieldMap;

    private final Map<String, ColumnDefinition> mappingFieldMap;
    private final Set<String> usedEntityFields;
    private final Set<String> usedMappingFields;
    private final Map<String, String> knownAliases;

    public DataSetInfoBuilder() {
      this.entityTableInfoMap = new HashMap<>();
      this.mappingTableInfoMap = new HashMap<>();
      this.entityFieldMap = new ConcurrentHashMap<>();
      this.mappingFieldMap = new ConcurrentHashMap<>();
      this.usedEntityFields = new HashSet<>();
      this.usedMappingFields = new HashSet<>();
      this.knownAliases = new HashMap<>();
      // we have to jump through a lot of hoops for associated_project fields to look like they are on the entity tables
      this.usedEntityFields.add("associated_project");
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
      // get rid of the synchronized field map because after this point it should be read only. so also make it unmodifyable
      return new DataSetInfo(
          entityTableInfoMap,
          mappingTableInfoMap,
          Collections.unmodifiableMap(new HashMap<>(entityFieldMap)),
          Collections.unmodifiableMap(new HashMap<>(mappingFieldMap)),
          usedEntityFields,
          usedMappingFields,
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
      List<String> primaryKeys = Collections.emptyList();
      if (tableNode.get("alter").has("primary_keys")) {
        primaryKeys = getPrimaryKeysFromJson(tableNode.get("alter").get("primary_keys"));
      }
      TableInfo.TableInfoBuilder builder =
          new TableInfo.TableInfoBuilder()
              .setTableName(tableName)
              .setColumnDefinitions(createColumnDefinitions(tableNode.get("columns"), tableName))
              .setPrimaryKeys(primaryKeys);
      // now we are defining mapping tables as any table with an _ except somatic_mutation
      boolean isMappingTable = tableName.contains("_") && !tableName.equals("somatic_mutation");
      builder.setIsMappingTable(isMappingTable);
      if (tableNode.get("alter").has("columns")) {
        builder.setTableRelationships(
            getRelationshipsFromJson(tableName, tableNode.get("alter").get("columns")));
      }
      TableInfo tableInfo = builder.build();
      addFieldsFromTable(tableInfo);

      // somatic_mutation table both an entity table and mapping table
      if (isMappingTable || tableName.equals("somatic_mutation")) {
        this.mappingTableInfoMap.put(tableName, tableInfo);
      }
      if (!isMappingTable) {
        this.entityTableInfoMap.put(tableName, tableInfo);
      }
    }

    private void addFieldsFromTable(TableInfo table) {
      String tableName = table.getTableName();
      ColumnDefinition[] cols = table.getColumnDefinitions();
      final boolean externalFields = !table.isMappingTable();

      if (tableName.contains("associated_project")) {
        Map<Boolean, List<ColumnDefinition>> partitionedList =
            Arrays.stream(cols)
                .collect(
                    Collectors.partitioningBy(c -> c.getName().contains("associated_project")));
        partitionedList.get(true).forEach(col -> addExternalFieldMapEntry(col, tableName));
        partitionedList.get(false).forEach(col -> addInternalFieldMapEntry(col, tableName));
      } else {
        Arrays.stream(cols)
            .filter(
                field ->
                   !(table.getRelationships().stream()
                       .map(rel -> rel.getFromField())
                       .collect(Collectors.toList()))
                       .contains(field.getName()))
            .forEach(
                col -> {
                  if (externalFields) {
                    addExternalFieldMapEntry(col, tableName);
                  } else {
                    addInternalFieldMapEntry(col, tableName);
                  }
                });
      }
    }

    private void addExternalFieldMapEntry(ColumnDefinition colDef, String tableName) {
      addFieldMapEntry(colDef, tableName, entityFieldMap, usedEntityFields);
    }

    private void addInternalFieldMapEntry(ColumnDefinition colDef, String tableName) {
      addFieldMapEntry(colDef, tableName, mappingFieldMap, usedMappingFields);
    }

    private void addFieldMapEntry(ColumnDefinition colDef, String tableName, Map<String, ColumnDefinition> fieldMap, Set<String> usedFields) {
      String fieldName = colDef.getName();
      if (tableName.contains("_associated_project") && fieldName.equals("associated_project")) {
        tableName = tableName.substring(0, tableName.indexOf("_associated_project"));
      }
      if (fieldMap.containsKey(fieldName) || usedFields.contains(fieldName)) {
          String alias = getNewFieldNameForDuplicate(fieldName, tableName);
          resolveFieldNameConflict(fieldName, fieldMap, usedFields);
        colDef.setAlias(alias);
        fieldName = alias;
      }
      fieldMap.put(fieldName, colDef);
    }

    public void resolveFieldNameConflict(String name, Map<String, ColumnDefinition> fieldMap, Set<String> usedFields) {
      if (fieldMap.containsKey(name)) {
        usedFields.add(name);
        ColumnDefinition col = fieldMap.get(name);
        String alias = getNewFieldNameForDuplicate(name, col.getTableName());
        fieldMap.remove(name);
        fieldMap.put(alias, col);
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
      String comment = "";
      if (colNode.has("comment")) {
          comment = colNode.get("comment").asText();
      }
      ColumnDefinition col = new ColumnDefinition(
              colNode.get("name").asText(),
              tableName,
              colNode.get("type").asText(),
              comment,
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
