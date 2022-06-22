package bio.terra.cda.app.models;

import bio.terra.cda.app.util.TableSchema;

public class QueryField {
  // region properties
  private final String name;
  private final String path;
  private final String[] parts;
  private final String alias;
  private final String columnText;
  private final boolean fileField;
  private final TableSchema.SchemaDefinition schemaDefinition;
  // endregion

  // region constructors
  public QueryField(
      String name,
      String path,
      String[] parts,
      String alias,
      String columnText,
      boolean fileField,
      TableSchema.SchemaDefinition schemaDefinition) {
    this.name = name;
    this.path = path;
    this.parts = parts;
    this.alias = alias;
    this.columnText = columnText;
    this.fileField = fileField;
    this.schemaDefinition = schemaDefinition;
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

  public boolean isFileField() {
    return this.fileField;
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
  // endregion
}
