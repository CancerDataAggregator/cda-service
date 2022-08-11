package bio.terra.cda.app.models;

import bio.terra.cda.app.util.TableSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class TableInfo {
    private final String tableName;
    private final String adjustedTableName;
    private final TableSchema.SchemaDefinition[] schemaDefinitions;
    private final List<TableRelationship> relationships;
    private final TableInfoTypeEnum type;

    private TableInfo(
            String tableName,
            String adjustedTableName,
            TableInfoTypeEnum type,
            TableSchema.SchemaDefinition[] schemaDefinitions) {
        this.tableName = tableName;
        this.adjustedTableName = adjustedTableName;
        this.relationships = new ArrayList<>();
        this.type = type;
        this.schemaDefinitions = schemaDefinitions;
    }

    public String getTableName() {
        return tableName;
    }

    public String getAdjustedTableName() { return adjustedTableName; }

    public void addRelationship(TableRelationship tableRelationship) {
        Optional<TableRelationship> optRelationship = relationships
                .stream()
                .filter(rel ->
                        rel.getTableInfo().getTableName().equals(tableRelationship.getTableInfo().getTableName())
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

    public enum TableInfoTypeEnum {
        NESTED,
        TABLE
    }

    public static TableInfo of(String tableName) {
        return new TableInfoBuilder().setTableName(tableName).build();
    }

    public static TableInfo of(String tableName, TableInfoTypeEnum type, TableSchema.SchemaDefinition[] schemaDefinitions) {
        return new TableInfoBuilder()
                .setTableName(tableName)
                .setType(type)
                .setSchemaDefinitions(Arrays.asList(schemaDefinitions))
                .build();
    }

    public static TableInfo of(String tableName, String adjustedTableName, TableInfoTypeEnum type, TableSchema.SchemaDefinition[] schemaDefinitions) {
        return new TableInfoBuilder()
                .setTableName(tableName)
                .setAdjustedTableName(adjustedTableName)
                .setType(type)
                .setSchemaDefinitions(Arrays.asList(schemaDefinitions))
                .build();
    }

    public static class TableInfoBuilder {
        private TableInfoTypeEnum type;
        private String tableName;
        private String adjustedTableName;
        private TableSchema.SchemaDefinition[] schemaDefinitions;

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

        public TableInfoBuilder setSchemaDefinitions(List<TableSchema.SchemaDefinition> schemaDefinitions) {
            this.schemaDefinitions = schemaDefinitions.toArray(TableSchema.SchemaDefinition[]::new);
            return this;
        }

        public TableInfo build() {
            return new TableInfo(this.tableName, this.adjustedTableName, this.type, this.schemaDefinitions);
        }

        public TableInfoBuilder setAdjustedTableName(String adjustedTableName) {
            this.adjustedTableName = adjustedTableName;
            return this;
        }
    }
}
