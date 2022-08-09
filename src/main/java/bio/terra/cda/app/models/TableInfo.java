package bio.terra.cda.app.models;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TableInfo {
    private final String tableName;
    private final List<TableRelationship> relationships;
    private final TableInfoTypeEnum type;

    private TableInfo(
            String tableName,
            TableInfoTypeEnum type) {
        this.tableName = tableName;
        this.relationships = new ArrayList<>();
        this.type = type;
    }

    public String getTableName() {
        return tableName;
    }

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

    public enum TableInfoTypeEnum {
        NESTED,
        TABLE
    }

    public static TableInfo of(String tableName) {
        return new TableInfoBuilder().setTableName(tableName).build();
    }

    public static TableInfo of(String tableName, TableInfoTypeEnum type) {
        return new TableInfoBuilder()
                .setTableName(tableName)
                .setType(type)
                .build();
    }

    public static class TableInfoBuilder {
        private TableInfoTypeEnum type;
        private String tableName;

        public TableInfoBuilder() {
            this.type = TableInfoTypeEnum.TABLE;
        }

        public TableInfoBuilder setType(TableInfoTypeEnum type) {
            this.type = type;
            return this;
        }

        public TableInfoBuilder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public TableInfo build() {
            return new TableInfo(this.tableName, this.type);
        }
    }
}
