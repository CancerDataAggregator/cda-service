package bio.terra.cda.app.models;

import bio.terra.cda.app.util.TableSchema;
import com.google.cloud.Tuple;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class DataSetInfo {
  private final Map<String, TableInfo> tableInfoMap;
  private final Map<String, FieldData> fieldMap;
  private final Map<String, String> knownAliases;

  private DataSetInfo(Map<String, TableInfo> tableInfoMap, Map<String, FieldData> fieldMap, Map<String, String> knownAliases) {
    this.tableInfoMap = tableInfoMap;
    this.fieldMap = fieldMap;
    this.knownAliases = knownAliases;
  }

  public TableInfo getTableInfo(String tableName) {
    TableInfo tableInfo = this.tableInfoMap.get(tableName);

    if (Objects.isNull(tableInfo)) {
      AtomicReference<String> table = new AtomicReference<>(tableName);
      this.knownAliases.forEach(
          (key, value) -> {
            if (value.equals(tableName)) {
              table.set(key);
            }
          });

      tableInfo = this.tableInfoMap.get(table.get());
    }

    return tableInfo;
  }

  public List<Map.Entry<String, String>> getFieldDescriptions() {
    Map<String, String> fieldDescs = new HashMap<>();

    this.fieldMap.entrySet().stream()
        .filter(
            entry ->
                !entry
                    .getValue()
                    .getSchemaDefinition()
                    .getType()
                    .equals(LegacySQLTypeName.RECORD.toString()))
        .forEach(
            entry ->
                fieldDescs.put(
                    entry.getKey(), entry.getValue().getSchemaDefinition().getDescription()));

    return fieldDescs.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .collect(Collectors.toList());
  }

  public Map<String, String> getKnownAliases() {
    return knownAliases;
  }

  public TableSchema.SchemaDefinition[] getSchemaDefinitionsForTable(String tableName) {
    TableInfo tableInfo = this.tableInfoMap.get(tableName);

    if (Objects.isNull(tableInfo)) {
      return null;
    }

    return tableInfo.getSchemaDefinitions();
  }

  public TableSchema.SchemaDefinition getSchemaDefinitionByFieldName(String fieldName) {
    FieldData fieldData = this.fieldMap.get(fieldName);

    if (Objects.isNull(fieldData)) {
      return null;
    }

    return fieldData.getSchemaDefinition();
  }

  public TableInfo getTableInfoFromField(String fieldName) {
    FieldData fieldData = this.fieldMap.get(fieldName);

    if (Objects.isNull(fieldData)) {
      return null;
    }

    return this.tableInfoMap.get(fieldData.getTableName());
  }

  public static String getNewNameForDuplicate(Map<String, String> knownAliases, String name, String tableName) {
    String prefix = knownAliases.getOrDefault(tableName, tableName);

    return String.join("_", List.of(prefix.toLowerCase(Locale.ROOT), name));
  }

  private static class FieldData {
    private final String tableName;
    private final TableSchema.SchemaDefinition schemaDefinition;

    private FieldData(String tableName, TableSchema.SchemaDefinition schemaDefinition) {
      this.tableName = tableName;
      this.schemaDefinition = schemaDefinition;
    }

    public String getTableName() {
      return tableName;
    }

    public TableSchema.SchemaDefinition getSchemaDefinition() {
      return schemaDefinition;
    }
  }

  public static class DataSetInfoBuilder {
    private final Map<String, TableInfo> tableInfoMap;
    private final Map<String, FieldData> fieldMap;
    private final Map<String, Boolean> usedFields;
    private final Map<String, String> knownAliases;

    public DataSetInfoBuilder() {
      this.tableInfoMap = new HashMap<>();
      this.fieldMap = new HashMap<>();
      this.usedFields = new HashMap<>();
      this.knownAliases = new HashMap<>();
    }

    public DataSetInfoBuilder addTableSchema(
            String tableName, TableSchema.TableDefinition tableDefinition) throws IOException {
      TableInfo tableInfo = this.tableInfoMap.get(tableName);
      if (Objects.isNull(tableInfo)) {
        TableSchema.SchemaDefinition partition =
            Arrays.stream(tableDefinition.getDefinitions())
                .filter(TableSchema.SchemaDefinition::getPartitionBy)
                .findFirst()
                .orElseThrow();

        this.tableInfoMap.put(
            tableDefinition.getTableAlias(),
            new TableInfo.TableInfoBuilder()
                .setTableName(tableName)
                .setType(TableInfo.TableInfoTypeEnum.TABLE)
                .setSchemaDefinitions(Arrays.stream(tableDefinition.getDefinitions()).collect(Collectors.toList()))
                .setPartitionKey(partition.getName())
                .setAdjustedTableName(tableDefinition.getTableAlias())
                .build());

        knownAliases.put(tableName, tableDefinition.getTableAlias());
      }

      TableInfo finalTableInfo = this.tableInfoMap.get(knownAliases.get(tableName));
      Queue<Tuple<TableInfo, TableSchema.SchemaDefinition>> queue =
            Arrays.stream(tableDefinition.getDefinitions())
              .map(schemaDefinition -> Tuple.of(finalTableInfo, schemaDefinition))
              .collect(Collectors.toCollection(LinkedList::new));
      while (!queue.isEmpty()) {
        Tuple<TableInfo, TableSchema.SchemaDefinition> tuple = queue.remove();
        tableInfo = tuple.x();

        TableSchema.SchemaDefinition definition = tuple.y();

        if (tableInfo.getTableName().equals("all_Files_v3_0_final")
            && List.of("ResearchSubject", "Specimen", "Subject").contains(definition.getName())) {
          continue;
        }

        ForeignKey[] foreignKeys = definition.getForeignKeys();
        Optional<List<TableRelationship.TableRelationshipBuilder>> relationshipsToAdd = Optional.empty();

        if (Objects.nonNull(foreignKeys)) {
          for (ForeignKey foreignKey : foreignKeys) {
            TableInfo fkTableInfo = this.tableInfoMap.get(knownAliases.get(foreignKey.getTableName()));

            if (Objects.isNull(fkTableInfo)) {
              this.addTableSchema(
                      foreignKey.getTableName(), TableSchema.getSchema(foreignKey.getTableName()));
              fkTableInfo = this.tableInfoMap.get(knownAliases.get(foreignKey.getTableName()));
            }

            if (definition.getMode().equals(Field.Mode.REPEATED.toString())) {
              if (relationshipsToAdd.isEmpty()) {
                relationshipsToAdd = Optional.of(new ArrayList<>());
              }

              relationshipsToAdd.get().add(new TableRelationship.TableRelationshipBuilder()
                                      .setDestinationTableInfo(fkTableInfo)
                                      .setParent(false)
                                      .setType(TableRelationship.TableRelationshipTypeEnum.JOIN)
                                      .setForeignKeys(List.of(foreignKey))
                                      .setField(definition.getName())
                                      .setArray(true));
            } else {
              tableInfo.addRelationship(
                      TableRelationship.of(
                                      tableInfo,
                                      definition.getName(),
                                      TableRelationship.TableRelationshipTypeEnum.JOIN,
                                      fkTableInfo)
                              .addForeignKey(foreignKey));
            }
          }
        }

        if (definition.getMode().equals(Field.Mode.REPEATED.toString())
        // && definition.getType().equals(LegacySQLTypeName.RECORD.toString())
        ) {
          String name = definition.getName();

          if (this.usedFields.containsKey(name)) {
            name = DataSetInfo.getNewNameForDuplicate(knownAliases, name, tableInfo.getTableName());

            TableInfo existingTableInfo = this.tableInfoMap.get(definition.getName());
            if (Objects.nonNull(existingTableInfo)) {
              TableRelationship existingParent =
                  existingTableInfo.getRelationships().stream()
                      .filter(TableRelationship::isParent)
                      .findFirst()
                      .orElseThrow();
              String newRecordName =
                  DataSetInfo.getNewNameForDuplicate(
                      knownAliases,
                      definition.getName(),
                      existingParent.getDestinationTableInfo().getTableName());

              existingTableInfo.setAdjustedTableName(newRecordName);
              this.tableInfoMap.put(newRecordName, existingTableInfo);
              this.tableInfoMap.remove(definition.getName());

              for (TableSchema.SchemaDefinition definition1 :
                  existingTableInfo.getSchemaDefinitions()) {
                if (this.usedFields.containsKey(definition1.getName())) {
                  String newDefName =
                      String.join("_", List.of(newRecordName, definition1.getName()));
                  definition1.setAlias(newDefName);

                  this.fieldMap.put(newDefName, new FieldData(newDefName, definition1));
                  this.fieldMap.remove(definition1.getName());
                }
              }

              FieldData existingFieldData = this.fieldMap.get(definition.getName());
              TableSchema.SchemaDefinition existingDefinition =
                  existingFieldData.getSchemaDefinition();
              existingDefinition.setAlias(newRecordName);

              this.fieldMap.put(newRecordName, new FieldData(newRecordName, existingDefinition));
              this.fieldMap.remove(definition.getName());
              definition.setAlias(newRecordName);
            }
          }

          TableSchema.SchemaDefinition partition = definition;
          TableInfo.TableInfoTypeEnum tableInfoType = TableInfo.TableInfoTypeEnum.ARRAY;
          TableSchema.SchemaDefinition[] fields = new TableSchema.SchemaDefinition[0];

          if (definition.getType().equals(LegacySQLTypeName.RECORD.toString())) {
            partition =
                Arrays.stream(definition.getFields())
                    .filter(TableSchema.SchemaDefinition::getPartitionBy)
                    .findFirst()
                    .orElseThrow();

            tableInfoType = TableInfo.TableInfoTypeEnum.NESTED;
            fields = definition.getFields();
          }

          TableInfo nested =
              TableInfo.of(definition.getName(), name, tableInfoType, fields, partition.getName());
          nested.addRelationship(
              TableRelationship.of(
                  nested,
                  name,
                  TableRelationship.TableRelationshipTypeEnum.UNNEST,
                  tableInfo,
                  true));
          tableInfo.addRelationship(
              TableRelationship.of(
                  tableInfo,
                  tableInfo.getTableName(),
                  TableRelationship.TableRelationshipTypeEnum.UNNEST,
                  nested));

          relationshipsToAdd.ifPresent(
              tableRelationshipBuilders ->
                  tableRelationshipBuilders.forEach(tableRelationshipBuilder ->
                          nested.addRelationship(
                            tableRelationshipBuilder.setFromTableInfo(nested).build())));

          this.tableInfoMap.put(name, nested);
          definition.setAlias(name);
          this.fieldMap.put(name, new FieldData(name, definition));
          this.usedFields.put(name, true);

          if (definition.getType().equals(LegacySQLTypeName.RECORD.toString())) {
            queue.addAll(
                Arrays.stream(definition.getFields())
                    .map(sd -> Tuple.of(nested, sd))
                    .collect(Collectors.toList()));
          }
        } else {
          String fieldName = definition.getName();

          if (this.usedFields.containsKey(fieldName)) {
            String prefix = tableInfo.getAdjustedTableName();

            if (knownAliases.containsKey(prefix)) {
              prefix = knownAliases.get(prefix);
            }

            fieldName = String.join("_", List.of(prefix.toLowerCase(Locale.ROOT), fieldName));

            FieldData initialField = this.fieldMap.get(definition.getName());
            if (Objects.nonNull(initialField)) {
              TableInfo tableInfo1 = this.tableInfoMap.get(initialField.getTableName());
              String newPrefix = tableInfo1.getAdjustedTableName();

              if (knownAliases.containsKey(newPrefix)) {
                newPrefix = knownAliases.get(newPrefix);
              }

              TableSchema.SchemaDefinition initialDefinition = initialField.getSchemaDefinition();
              String newName =
                  String.join(
                      "_", List.of(newPrefix.toLowerCase(Locale.ROOT), definition.getName()));

              initialDefinition.setAlias(newName);
              this.fieldMap.put(
                  newName, new FieldData(initialField.getTableName(), initialDefinition));
              this.fieldMap.remove(definition.getName());
              definition.setAlias(newName);
            }
          }

          definition.setAlias(fieldName);
          this.fieldMap.put(fieldName, new FieldData(tableInfo.getAdjustedTableName(), definition));
          this.usedFields.put(fieldName, true);
        }
      }
      return this;
    }

    public DataSetInfo build() {
      return new DataSetInfo(tableInfoMap, fieldMap, knownAliases);
    }
  }
}
