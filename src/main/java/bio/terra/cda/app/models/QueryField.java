package bio.terra.cda.app.models;

public class QueryField {
  // region properties
  private final String name;
  private final String path;
  private final String alias;
//  private final String columnText;
  private final ColumnDefinition columnDefinition;
  private final boolean filesQuery;
  private final String tableName;
  private final boolean fileField;
  private final String modifier;
  // endregion

  // region constructors
  public QueryField(
      String name,
      String path,
      String alias,
      String tableName,
      String modifier,
      ColumnDefinition columnDefinition,
      boolean filesQuery,
      boolean fileField) {
    this.name = name;
    this.path = path;
    this.alias = alias;
    this.columnDefinition = columnDefinition;
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

  public String getAlias() {
    return this.alias;
  }

  public ColumnDefinition getColumn() {
    return columnDefinition;
  }

  public String getType() {
    return this.columnDefinition.getType();
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
