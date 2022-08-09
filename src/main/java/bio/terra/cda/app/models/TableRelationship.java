package bio.terra.cda.app.models;

import java.util.ArrayList;
import java.util.List;

public class TableRelationship {
    private final String field;
    private final TableRelationshipTypeEnum type;
    private final TableInfo tableInfo;
    private final List<ForeignKey> foreignKeys;
    private final boolean parent;

    private TableRelationship(String field, TableRelationshipTypeEnum type, TableInfo tableInfo, boolean parent) {
        this.field = field;
        this.type = type;
        this.foreignKeys = new ArrayList<>();
        this.tableInfo = tableInfo;
        this.parent = parent;
    }

    public TableRelationshipTypeEnum getType() {
        return type;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public List<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    public TableRelationship addForeignKey(ForeignKey foreignKey) {
        this.foreignKeys.add(foreignKey);
        return this;
    }

    public static TableRelationship of(String field, TableRelationshipTypeEnum type, TableInfo tableInfo) {
        return new TableRelationshipBuilder().setField(field).setType(type).setTableInfo(tableInfo).build();
    }

    public static TableRelationship of(String field, TableRelationshipTypeEnum type, TableInfo tableInfo, boolean parent) {
        return new TableRelationshipBuilder()
                .setField(field)
                .setType(type)
                .setTableInfo(tableInfo)
                .setParent(parent)
                .build();
    }

    public String getField() {
        return field;
    }

    public boolean isParent() {
        return parent;
    }

    public enum TableRelationshipTypeEnum {
        UNNEST("UNNEST"),
        JOIN("JOIN");

        private String value;

        TableRelationshipTypeEnum(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    public static class TableRelationshipBuilder {
        private TableRelationshipTypeEnum type;
        private TableInfo tableInfo;
        private String field;
        private boolean parent;

        public TableRelationshipBuilder() {
            this.parent = false;
        }

        public TableRelationshipBuilder setType(TableRelationshipTypeEnum type) {
            this.type = type;
            return this;
        }

        public TableRelationshipBuilder setTableInfo(TableInfo tableInfo) {
            this.tableInfo = tableInfo;
            return this;
        }

        public TableRelationshipBuilder setField(String field) {
            this.field = field;
            return this;
        }

        public TableRelationshipBuilder setParent(boolean parent) {
            this.parent = parent;
            return this;
        }

        public TableRelationship build() {
            return new TableRelationship(this.field, this.type, this.tableInfo, this.parent);
        }
    }
}
