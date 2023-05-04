package bio.terra.cda.app.models;

import bio.terra.cda.app.builders.ColumnsReturnBuilder;
import bio.terra.cda.app.generators.QueryGenerator;
import bio.terra.cda.app.util.EndpointUtil;
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
import java.util.stream.Collectors;

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
  private final Map<String, TableInfo> tableInfoMap;
  private final Map<String, FieldData> fieldMap;
  private final Map<String, String> knownAliases;

  private DataSetInfo(
      Map<String, TableInfo> tableInfoMap,
      Map<String, FieldData> fieldMap,
      Map<String, String> knownAliases) {
    this.tableInfoMap = tableInfoMap;
    this.fieldMap = fieldMap;
    this.knownAliases = knownAliases;
  }

  public TableInfo getTableInfo(String tableName) {
    return this.tableInfoMap.get(this.knownAliases.getOrDefault(tableName, tableName));
  }

  public List<ColumnsReturn> getColumnsData(ColumnsReturnBuilder columnsReturnBuilder) {
    return this.fieldMap.entrySet().stream()
        .filter(
            entry -> {
              TableSchema.SchemaDefinition schemaDefinition =
                  entry.getValue().getSchemaDefinition();

              return !schemaDefinition.getType().equals(LegacySQLTypeName.RECORD.toString())
                  && !schemaDefinition.isExcludeFromSelect();
            })
        .map(
            entry ->
                columnsReturnBuilder.of(
                    validateAndGetEndpoint(entry.getValue().getTableInfo()),
                    entry.getKey(),
                    entry.getValue().getSchemaDefinition().getDescription(),
                    entry.getValue().getSchemaDefinition().getType(),
                    entry.getValue().getSchemaDefinition().getMode()))
        .collect(Collectors.toList());
  }

  private String validateAndGetEndpoint(TableInfo endpointTable) {
    List<String> endpoints =
        EndpointUtil.getQueryGeneratorClasses()
            .map(
                cls -> {
                  QueryGenerator generator = cls.getAnnotation(QueryGenerator.class);
                  return generator.entity();
                })
            .collect(Collectors.toList());
    endpoints.add(TableSchema.FILE_PREFIX);

    if (endpoints.contains(endpointTable.getAdjustedTableName())) {
      return endpointTable.getAdjustedTableName().toLowerCase(Locale.ROOT);
    } else {
      Queue<TableInfo> tableInfoQueue = new LinkedList<>(List.of(endpointTable));

      while (!tableInfoQueue.isEmpty()) {
        TableInfo current = tableInfoQueue.remove();

        if (endpoints.contains(current.getAdjustedTableName())) {
          return current.getAdjustedTableName();
        }

        Optional<TableRelationship> parent =
            current.getRelationships().stream().filter(TableRelationship::isParent).findFirst();

        parent.ifPresent(
            tableRelationship -> tableInfoQueue.add(tableRelationship.getDestinationTableInfo()));
      }

      return null;
    }
  }

  public Map<String, String> getKnownAliases() {
    return knownAliases;
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

    return this.getTableInfo(fieldData.getTableInfo().getAdjustedTableName());
  }

  public static String getNewNameForDuplicate(
      Map<String, String> knownAliases, String name, String tableName) {
    String prefix = knownAliases.getOrDefault(tableName, tableName);

    return String.join("_", List.of(prefix.toLowerCase(Locale.ROOT), name));
  }

  private static class FieldData {
    private final TableInfo tableInfo;
    private final TableSchema.SchemaDefinition schemaDefinition;

    private FieldData(TableInfo tableInfo, TableSchema.SchemaDefinition schemaDefinition) {
      this.tableInfo = tableInfo;
      this.schemaDefinition = schemaDefinition;
    }

    public TableInfo getTableInfo() {
      return tableInfo;
    }

    public TableSchema.SchemaDefinition getSchemaDefinition() {
      return schemaDefinition;
    }
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
      TableInfo tableInfo = this.addTableIfNotExists(tableName, tableDefinition);

      Queue<Tuple<TableInfo, TableSchema.SchemaDefinition>> queue = new LinkedList<>();
      for (TableSchema.SchemaDefinition schemaDefinition : tableDefinition.getDefinitions()) {
        queue.add(Tuple.of(tableInfo, schemaDefinition));
      }

      while (!queue.isEmpty()) {
        Tuple<TableInfo, TableSchema.SchemaDefinition> tuple = queue.remove();
        tableInfo = tuple.x();

        TableSchema.SchemaDefinition definition = tuple.y();

        if (tableInfo.getTableName().equals("all_Files_v3_0_final")
            && List.of("ResearchSubject", "Specimen", "Subject").contains(definition.getName())) {
          continue;
        }

        ForeignKey[] foreignKeys = definition.getForeignKeys();
        Optional<List<TableRelationship.TableRelationshipBuilder>> relationshipsToAdd =
            Optional.empty();

        if (Objects.nonNull(foreignKeys)) {
          for (ForeignKey foreignKey : foreignKeys) {
            TableRelationship.TableRelationshipBuilder builder =
                this.addRelationship(foreignKey, definition, tableInfo);

            if (Objects.nonNull(builder)) {
              if (relationshipsToAdd.isEmpty()) {
                relationshipsToAdd = Optional.of(new ArrayList<>());
              }

              relationshipsToAdd.get().add(builder);
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

                  this.fieldMap.put(newDefName, new FieldData(existingTableInfo, definition1));
                  this.fieldMap.remove(definition1.getName());
                }
              }

              FieldData existingFieldData = this.fieldMap.get(definition.getName());
              TableSchema.SchemaDefinition existingDefinition =
                  existingFieldData.getSchemaDefinition();
              existingDefinition.setAlias(newRecordName);

              this.fieldMap.put(
                  newRecordName, new FieldData(existingTableInfo, existingDefinition));
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
                  tableRelationshipBuilders.forEach(
                      tableRelationshipBuilder ->
                          nested.addRelationship(
                              tableRelationshipBuilder.setFromTableInfo(nested).build())));

          this.tableInfoMap.put(name, nested);
          definition.setAlias(name);
          setCountByTableInfo(definition, nested);
          this.fieldMap.put(name, new FieldData(nested, definition));
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
              TableInfo tableInfo1 = initialField.getTableInfo();

              String newPrefix = tableInfo1.getAdjustedTableName();

              if (tableInfo1.getType().equals(TableInfo.TableInfoTypeEnum.ARRAY)) {
                newPrefix =
                    tableInfo1.getRelationships().stream()
                        .filter(TableRelationship::isParent)
                        .findFirst()
                        .orElseThrow()
                        .getDestinationTableInfo()
                        .getAdjustedTableName();
              }

              if (knownAliases.containsKey(newPrefix)) {
                newPrefix = knownAliases.get(newPrefix);
              }

              TableSchema.SchemaDefinition initialDefinition = initialField.getSchemaDefinition();
              String newName =
                  String.join(
                      "_", List.of(newPrefix.toLowerCase(Locale.ROOT), definition.getName()));

              initialDefinition.setAlias(newName);
              this.fieldMap.put(newName, new FieldData(tableInfo1, initialDefinition));
              this.fieldMap.remove(definition.getName());
              definition.setAlias(newName);
            }
          }

          definition.setAlias(fieldName);
          setCountByTableInfo(definition, tableInfo);
          this.fieldMap.put(fieldName, new FieldData(tableInfo, definition));
          this.usedFields.put(fieldName, true);
        }
      }
      return this;
    }

    public DataSetInfo build() {
      return new DataSetInfo(tableInfoMap, fieldMap, knownAliases);
    }

    private TableInfo addTableIfNotExists(
        String tableName, TableSchema.TableDefinition tableDefinition) {
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
                .setSchemaDefinitions(
                    Arrays.stream(tableDefinition.getDefinitions()).collect(Collectors.toList()))
                .setPartitionKey(partition.getName())
                .setAdjustedTableName(tableDefinition.getTableAlias())
                .build());

        knownAliases.put(tableName, tableDefinition.getTableAlias());
      }

      return this.tableInfoMap.get(this.knownAliases.getOrDefault(tableName, tableName));
    }

    private TableRelationship.TableRelationshipBuilder addRelationship(
        ForeignKey foreignKey, TableSchema.SchemaDefinition definition, TableInfo tableInfo)
        throws IOException {
      TableInfo fkTableInfo =
          this.tableInfoMap.get(
              this.knownAliases.getOrDefault(foreignKey.getTableName(), foreignKey.getTableName()));

      if (Objects.isNull(fkTableInfo)) {
        this.addTableSchema(
            foreignKey.getTableName(), TableSchema.getSchema(foreignKey.getTableName()));
        fkTableInfo =
            this.tableInfoMap.get(
                this.knownAliases.getOrDefault(
                    foreignKey.getTableName(), foreignKey.getTableName()));
      }

      if (definition.getMode().equals(Field.Mode.REPEATED.toString())) {
        return new TableRelationship.TableRelationshipBuilder()
            .setDestinationTableInfo(fkTableInfo)
            .setParent(false)
            .setType(TableRelationship.TableRelationshipTypeEnum.JOIN)
            .setForeignKeys(List.of(foreignKey))
            .setField(definition.getName())
            .setArray(true);
      } else {
        tableInfo.addRelationship(
            TableRelationship.of(
                    tableInfo,
                    definition.getName(),
                    TableRelationship.TableRelationshipTypeEnum.JOIN,
                    fkTableInfo)
                .addForeignKey(foreignKey));
        return null;
      }
    }

    private void setCountByTableInfo(TableSchema.SchemaDefinition definition, TableInfo tableInfo) {
      CountByField[] countByFields = definition.getCountByFields();
      if (Objects.nonNull(countByFields) && countByFields.length > 0) {
        for (CountByField countByField : countByFields) {
          if (Objects.isNull(countByField.getTable())) {
            countByField.setTableInfo(tableInfo);
          } else {
            TableInfo countByTableInfo =
                this.tableInfoMap.get(
                    this.knownAliases.getOrDefault(
                        countByField.getTable(), countByField.getTable()));

            countByField.setTableInfo(countByTableInfo);
          }
        }
      }
    }
  }
}
