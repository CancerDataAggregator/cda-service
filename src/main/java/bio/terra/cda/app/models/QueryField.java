package bio.terra.cda.app.models;

import bio.terra.cda.app.util.TableSchema;

public class QueryField {
  // region properties
  private final String name;
  private final String path;
  private final String[] parts;
  private final String alias;
  private final String columnText;
  private final TableSchema.SchemaDefinition schemaDefinition;
  private final boolean filesQuery;
  private final String tableName;
  private final boolean fileField;
  private final String modifier;
  // endregion

  // region constructors
  public QueryField(
      String name,
      String path,
      String[] parts,
      String alias,
      String columnText,
      String tableName,
      String modifier,
      TableSchema.SchemaDefinition schemaDefinition,
      boolean filesQuery,
      boolean fileField) {
    this.name = name;
    this.path = path;
    this.parts = parts;
    this.alias = alias;
    this.columnText = columnText;
    this.schemaDefinition = schemaDefinition;
    this.filesQuery = filesQuery;
    this.tableName = tableName;
    this.fileField = fileField;
    this.modifier = modifier;
  }
  // endregion

  // region getter and setters
  public String getName() {
    return this.name;
  }

  public String getPath() {
    return this.path;
  }

  public String[] getParts() {
    return this.parts;
  }

  public String getAlias() {
    return this.alias;
  }

  public String getColumnText() {
    return this.columnText;
  }

  public String getMode() {
    return this.schemaDefinition.getMode();
  }

  public String getType() {
    return this.schemaDefinition.getType();
  }

  public TableSchema.SchemaDefinition[] getFields() {
    return this.schemaDefinition.getFields();
  }

  public boolean isFilesQuery() {
    return filesQuery;
  }

  public String getTableName() {
    return tableName;
  }

  public boolean isFileField() {
    return fileField;
  }

  public String getModifier() {
    return modifier;
  }
  // endregion
}
