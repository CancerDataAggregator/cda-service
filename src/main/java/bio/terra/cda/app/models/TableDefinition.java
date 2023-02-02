package bio.terra.cda.app.models;

public class TableDefinition {
    private String tableAlias;
    private SchemaDefinition[] definitions;

    public String getTableAlias() {
        return this.tableAlias;
    }

    public SchemaDefinition[] getDefinitions() {
        return definitions;
    }
}
