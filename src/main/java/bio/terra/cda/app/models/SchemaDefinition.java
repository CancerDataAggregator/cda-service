package bio.terra.cda.app.models;

import java.util.Objects;

public class SchemaDefinition {
    private String mode;
    private String name;
    private String type;
    private String description;
    private SchemaDefinition[] fields;
    private ForeignKey[] foreignKeys;
    private Boolean partitionBy;
    private String alias;
    private CountByField[] countByFields;
    private boolean excludeFromSelect;

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setFields(SchemaDefinition[] fields) {
        this.fields = fields;
    }

    public SchemaDefinition[] getFields() {
        return this.fields;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDescription() {
        return this.description;
    }

    public ForeignKey[] getForeignKeys() {
        return foreignKeys;
    }

    public void setForeignKeys(ForeignKey[] foreignKeys) {
        this.foreignKeys = foreignKeys;
    }

    public Boolean getPartitionBy() {
        return !Objects.isNull(partitionBy) && partitionBy;
    }

    public void setPartitionBy(Boolean partitionBy) {
        this.partitionBy = partitionBy;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public CountByField[] getCountByFields() {
        return countByFields;
    }

    public void setCountByFields(CountByField[] countByFields) {
        this.countByFields = countByFields;
    }

    public boolean isExcludeFromSelect() {
        return excludeFromSelect;
    }

    public void setExcludeFromSelect(boolean excludeFromSelect) {
        this.excludeFromSelect = excludeFromSelect;
    }
}
