package bio.terra.cda.app.models;

import bio.terra.cda.app.util.TableSchema;
import com.google.cloud.Tuple;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;

public class DataSetInfo {
    private final Map<String, TableInfo> tableInfoMap;
    private final Map<String, FieldData> fieldMap;
    public static final String ID_COLUMN = "id";
    public static final String FILES_COLUMN = "Files";
    public static final String IDENTIFIER_COLUMN = "identifier";
    public static final String SYSTEM_IDENTIFIER = "identifier.system";
    public static final Map<String, String> KNOWN_ALIASES = new HashMap<>() {{
        put("all_Subjects_v3_0_final", "Subject");
        put("all_Files_v3_0_final", "File");
        put("somatic_mutation_hg38_gdc_current", "Mutation");
    }};

    private DataSetInfo(Map<String, TableInfo> tableInfoMap,
                        Map<String, FieldData> fieldMap) {
        this.tableInfoMap = tableInfoMap;
        this.fieldMap = fieldMap;
    }

    public TableInfo getTableInfo(String tableName) {
        return this.tableInfoMap.get(tableName);
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
        private final Map<String, String> switchedTables;

        public DataSetInfoBuilder() {
            this.tableInfoMap = new HashMap<>();
            this.fieldMap = new HashMap<>();
            this.usedFields = new HashMap<>();
            this.switchedTables = new HashMap<>();
        }

        public DataSetInfoBuilder addTableSchema(String tableName, List<TableSchema.SchemaDefinition> tableSchema) throws IOException {
            TableInfo tableInfo = this.tableInfoMap.get(tableName);
            if (Objects.isNull(tableInfo)) {
                this.tableInfoMap.put(tableName, new TableInfo.TableInfoBuilder()
                        .setTableName(tableName)
                        .setType(TableInfo.TableInfoTypeEnum.TABLE)
                        .setSchemaDefinitions(tableSchema)
                        .build());
            }

            TableInfo finalTableInfo = this.tableInfoMap.get(tableName);
            Queue<Tuple<TableInfo, TableSchema.SchemaDefinition>> queue =
                    tableSchema.stream()
                            .map(schemaDefinition -> Tuple.of(finalTableInfo, schemaDefinition))
                            .collect(Collectors.toCollection(LinkedList::new));
            while (!queue.isEmpty()) {
                Tuple<TableInfo, TableSchema.SchemaDefinition> tuple = queue.remove();
                tableInfo = tuple.x();

                String newTable = this.switchedTables.get(tableInfo.getTableName());
                if (Objects.nonNull(newTable)) {
                    tableInfo = this.tableInfoMap.get(newTable);
                }

                TableSchema.SchemaDefinition definition = tuple.y();

                if (tableName.equals("all_Files_v3_0_final")
                        && List.of("ResearchSubject", "Specimen", "Subject")
                        .contains(definition.getName())) {
                    continue;
                }

                ForeignKey foreignKey = definition.getForeignKey();
                if (Objects.nonNull(foreignKey)){
                    TableInfo fkTableInfo = this.tableInfoMap.get(foreignKey.getTableName());

                    if (Objects.isNull(fkTableInfo)) {
                        this.addTableSchema(foreignKey.getTableName(), TableSchema.getSchema(foreignKey.getTableName()));
                        fkTableInfo = this.tableInfoMap.get(foreignKey.getTableName());
                    }

                    tableInfo.addRelationship(
                            TableRelationship.of(definition.getName(),
                                    TableRelationship.TableRelationshipTypeEnum.JOIN,
                                            fkTableInfo).addForeignKey(foreignKey));
                }

                if (definition.getMode().equals(Field.Mode.REPEATED.toString())
                        && definition.getType().equals(LegacySQLTypeName.RECORD.toString())) {
                    String name = definition.getName();

                    if (this.usedFields.containsKey(name)) {
                        String prefix = KNOWN_ALIASES.containsKey(tableInfo.getTableName())
                                ? KNOWN_ALIASES.get(tableInfo.getTableName())
                                : tableInfo.getTableName();

                        name = String.join("_", List.of(prefix.toLowerCase(Locale.ROOT), name));

                        TableInfo existingTableInfo = this.tableInfoMap.get(definition.getName());
                        if (Objects.nonNull(existingTableInfo)) {
                            TableRelationship existingParent = existingTableInfo
                                    .getRelationships().stream().filter(TableRelationship::isParent)
                                    .findFirst().orElseThrow();
                            String newRecordPrefix = existingParent.getTableInfo().getTableName();
                            if (KNOWN_ALIASES.containsKey(newRecordPrefix)) {
                                newRecordPrefix = KNOWN_ALIASES.get(newRecordPrefix);
                            }

                            String newRecordName = String.join(
                                    "_", List.of(newRecordPrefix.toLowerCase(Locale.ROOT), definition.getName()));
                            this.tableInfoMap.put(newRecordName,
                                    TableInfo.of(existingTableInfo.getTableName(),
                                            newRecordName,
                                            existingTableInfo.getType(),
                                            existingTableInfo.getSchemaDefinitions()));
                            this.tableInfoMap.remove(definition.getName());
                            this.switchedTables.put(definition.getName(), newRecordName);

                            for (TableSchema.SchemaDefinition definition1 : existingTableInfo.getSchemaDefinitions()) {
                                if (this.usedFields.containsKey(definition1.getName())) {
                                    FieldData fieldData = this.fieldMap.get(definition1.getName());
                                    String newDefName = String.join("_",
                                            List.of(newRecordName, definition1.getName()));
                                    this.fieldMap.put(
                                            newDefName,
                                            new FieldData(newDefName, fieldData.getSchemaDefinition()));
                                    this.fieldMap.remove(definition1.getName());
                                }
                            }

                            FieldData existingFieldData = this.fieldMap.get(definition.getName());
                            this.fieldMap.put(newRecordName, new FieldData(newRecordName, existingFieldData.getSchemaDefinition()));
                            this.fieldMap.remove(definition.getName());
                        }
                    }

                    TableInfo nested = TableInfo.of(
                            definition.getName(),
                            name,
                            TableInfo.TableInfoTypeEnum.NESTED,
                            definition.getFields());
                    nested.addRelationship(
                            TableRelationship.of(
                                    name, TableRelationship.TableRelationshipTypeEnum.UNNEST, tableInfo, true));
                    tableInfo.addRelationship(
                            TableRelationship.of(
                                    tableInfo.getTableName(), TableRelationship.TableRelationshipTypeEnum.UNNEST, nested));

                    this.tableInfoMap.put(name, nested);
                    this.fieldMap.put(name, new FieldData(name, definition));
                    this.usedFields.put(name, true);

                    queue.addAll(Arrays.stream(definition.getFields())
                            .map(sd -> Tuple.of(nested, sd)).collect(Collectors.toList()));
                } else {
                    String fieldName = definition.getName();

                    if (this.usedFields.containsKey(fieldName)) {
                        String prefix = tableInfo.getAdjustedTableName();

                        if (KNOWN_ALIASES.containsKey(prefix)){
                            prefix = KNOWN_ALIASES.get(prefix);
                        }

                        fieldName = String.join("_", List.of(prefix.toLowerCase(Locale.ROOT), fieldName));

                        FieldData initialField = this.fieldMap.get(definition.getName());
                        if (Objects.nonNull(initialField)) {
                            TableInfo tableInfo1 = this.tableInfoMap.get(initialField.getTableName());
                            String newPrefix = tableInfo1.getAdjustedTableName();

                            if (KNOWN_ALIASES.containsKey(newPrefix)){
                                newPrefix = KNOWN_ALIASES.get(newPrefix);
                            }

                            String newName = String.join("_", List.of(newPrefix.toLowerCase(Locale.ROOT), definition.getName()));
                            this.fieldMap.put(newName, new FieldData(initialField.getTableName(), initialField.getSchemaDefinition()));
                            this.fieldMap.remove(definition.getName());
                        }
                    }

                    this.fieldMap.put(fieldName, new FieldData(tableInfo.getAdjustedTableName(), definition));
                    this.usedFields.put(fieldName, true);
                }
            }
            return this;
        }

        public DataSetInfo build() {
            return new DataSetInfo(tableInfoMap, fieldMap);
        }
    }
}
