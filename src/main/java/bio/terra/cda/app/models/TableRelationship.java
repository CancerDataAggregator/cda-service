package bio.terra.cda.app.models;

import java.util.ArrayList;
import java.util.List;

public class TableRelationship {
    private final String field;
    private final TableRelationshipTypeEnum type;
    private final TableInfo fromTableInfo;
    private final TableInfo destinationTableInfo;
    private final List<ForeignKey> foreignKeys;
    private final boolean parent;

    private TableRelationship(TableInfo fromTableInfo, String field, TableRelationshipTypeEnum type, TableInfo destinationTableInfo, boolean parent) {
        this.fromTableInfo = fromTableInfo;
        this.field = field;
        this.type = type;
        this.foreignKeys = new ArrayList<>();
        this.destinationTableInfo = destinationTableInfo;
        this.parent = parent;
    }

    public TableRelationshipTypeEnum getType() {
        return type;
    }

    public TableInfo getFromTableInfo() { return fromTableInfo; }

    public TableInfo getDestinationTableInfo() {
        return destinationTableInfo;
    }

    public List<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    public TableRelationship addForeignKey(ForeignKey foreignKey) {
        this.foreignKeys.add(foreignKey);
        return this;
    }

    public static TableRelationship of(TableInfo fromTableInfo, String field, TableRelationshipTypeEnum type, TableInfo destinationTableInfo) {
        return new TableRelationshipBuilder()
                .setField(field)
                .setType(type)
                .setFromTableInfo(fromTableInfo)
                .setDestinationTableInfo(destinationTableInfo)
                .build();
    }

    public static TableRelationship of(TableInfo fromTableInfo, String field, TableRelationshipTypeEnum type, TableInfo destinationTableInfo, boolean parent) {
        return new TableRelationshipBuilder()
                .setField(field)
                .setType(type)
                .setFromTableInfo(fromTableInfo)
                .setDestinationTableInfo(destinationTableInfo)
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
        private TableInfo fromTableInfo;
        private TableInfo destinationTableInfo;
        private String field;
        private boolean parent;

        public TableRelationshipBuilder() {
            this.parent = false;
        }

        public TableRelationshipBuilder setType(TableRelationshipTypeEnum type) {
            this.type = type;
            return this;
        }

        public TableRelationshipBuilder setFromTableInfo(TableInfo tableInfo) {
            this.fromTableInfo = tableInfo;
            return this;
        }

        public TableRelationshipBuilder setDestinationTableInfo(TableInfo tableInfo) {
            this.destinationTableInfo = tableInfo;
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
            return new TableRelationship(
                    this.fromTableInfo, this.field, this.type, this.destinationTableInfo, this.parent);
        }
    }
}
