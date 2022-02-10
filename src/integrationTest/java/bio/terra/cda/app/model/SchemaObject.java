package bio.terra.cda.app.model;

public class SchemaObject {
  private String id;
  private Identifier identifier;
  private String table_catalog;
  private String table_schema;
  private String table_name;
  private String column_name;
  private String field_path;
  private String data_type;
  private String description;

  public String getTable_catalog() {
    return table_catalog;
  }

  public void setTable_catalog(String table_catalog) {
    this.table_catalog = table_catalog;
  }

  public String getTable_schema() {
    return table_schema;
  }

  public void setTable_schema(String table_schema) {
    this.table_schema = table_schema;
  }

  public String getTable_name() {
    return table_name;
  }

  public void setTable_name(String table_name) {
    this.table_name = table_name;
  }

  public String getColumn_name() {
    return column_name;
  }

  public void setColumn_name(String column_name) {
    this.column_name = column_name;
  }

  public String getField_path() {
    return field_path;
  }

  public void setField_path(String field_path) {
    this.field_path = field_path;
  }

  public String getData_type() {
    return data_type;
  }

  public void setData_type(String data_type) {
    this.data_type = data_type;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Identifier getIdentifier() {
    return identifier;
  }

  public void setIdentifier(Identifier identifier) {
    this.identifier = identifier;
  }

  @Override
  public String toString() {
    return "SchemaObject{"
        + "table_catalog='"
        + table_catalog
        + '\''
        + ", table_schema='"
        + table_schema
        + '\''
        + ", table_name='"
        + table_name
        + '\''
        + ", column_name='"
        + column_name
        + '\''
        + ", field_path='"
        + field_path
        + '\''
        + ", data_type='"
        + data_type
        + '\''
        + ", description='"
        + description
        + '\''
        + '}';
  }
}
