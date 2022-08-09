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
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;

public class DataSetInfo {
    private final Map<String, TableSchema.SchemaDefinition[]> tableSchemas;
    private final Map<String, TableInfo> tableInfoMap;

    private DataSetInfo(Map<String, TableSchema.SchemaDefinition[]> tableSchemas,
                        Map<String, TableInfo> tableInfoMap) {
        this.tableSchemas = tableSchemas;
        this.tableInfoMap = tableInfoMap;
    }

    public TableInfo getTableInfo(String tableName) {
        return this.tableInfoMap.get(tableName);
    }

    public TableSchema.SchemaDefinition[] getSchemaDefinitions(String tableName) {
        return this.tableSchemas.get(tableName);
    }

    public static class DataSetInfoBuilder {
        private final Map<String, TableSchema.SchemaDefinition[]> tableSchemas;
        private final Map<String, TableInfo> tableInfoMap;

        public DataSetInfoBuilder() {
            this.tableSchemas = new HashMap<>();
            this.tableInfoMap = new HashMap<>();
        }

        public DataSetInfoBuilder addTableSchema(String tableName, List<TableSchema.SchemaDefinition> tableSchema) throws IOException {
            this.tableSchemas.put(tableName, tableSchema.toArray(TableSchema.SchemaDefinition[]::new));

            TableInfo tableInfo = this.tableInfoMap.get(tableName);
            if (Objects.isNull(tableInfo)) {
                this.tableInfoMap.put(tableName, new TableInfo.TableInfoBuilder()
                        .setTableName(tableName)
                        .setType(TableInfo.TableInfoTypeEnum.TABLE)
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
                TableSchema.SchemaDefinition definition = tuple.y();

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
                        && definition.getType().equals(LegacySQLTypeName.RECORD.toString())
                        && !(tableName.equals("all_Files_v3_0_final")
                            && List.of("ResearchSubject", "Specimen", "Subject")
                                   .contains(definition.getName()))) {
                    TableInfo nested = TableInfo.of(
                            definition.getName(), TableInfo.TableInfoTypeEnum.NESTED);
                    nested.addRelationship(
                            TableRelationship.of(
                                    definition.getName(), TableRelationship.TableRelationshipTypeEnum.UNNEST, tableInfo, true));
                    tableInfo.addRelationship(
                            TableRelationship.of(
                                    tableInfo.getTableName(), TableRelationship.TableRelationshipTypeEnum.UNNEST, nested));

                    this.tableSchemas.put(definition.getName(), definition.getFields());
                    this.tableInfoMap.put(definition.getName(), nested);
                    queue.addAll(Arrays.stream(definition.getFields())
                            .map(sd -> Tuple.of(nested, sd)).collect(Collectors.toList()));
                }
            }
            return this;
        }

        public DataSetInfo build() {
            return new DataSetInfo(tableSchemas, tableInfoMap);
        }
    }
}
