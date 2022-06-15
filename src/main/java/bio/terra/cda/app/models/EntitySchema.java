package bio.terra.cda.app.models;

import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

public class EntitySchema {
  // region properties
  private String path;
  private TableSchema.SchemaDefinition schema;
  private String table;
  public static final String DEFAULT_PATH = "Subject";
  private String[] parts;
  private String prefix;
  // endregion

  // region constructor
  public EntitySchema() {}

  public EntitySchema(String path, TableSchema.SchemaDefinition schemaDefinition) {
    setPath(path);
    setSchema(schemaDefinition);
  }
  // endregion

  // region getters and setters
  public String getTable() {
    return this.table;
  }

  public EntitySchema setTable(String table) {
    this.table = table;
    return this;
  }

  public String getPath() {
    if (Objects.isNull(this.path)) {
      return DEFAULT_PATH;
    }

    return this.path;
  }

  public EntitySchema setPath(String path) {
    this.path = path;
    setParts();
    setPrefix();
    return this;
  }

  public TableSchema.SchemaDefinition getSchema() {
    return this.schema;
  }

  public EntitySchema setSchema(TableSchema.SchemaDefinition schema) {
    this.schema = schema;
    return this;
  }
  // endregion

  // region public methods
  public Boolean wasFound() {
    return Objects.nonNull(path);
  }

  public String[] getParts() {
    return wasFound() ? this.parts : new String[0];
  }

  private void setParts() {
    this.parts = Objects.nonNull(path) ? SqlUtil.getParts(path) : new String[0];
  }

  public String getPrefix() {
    return wasFound() ? this.prefix : getTable();
  }

  private void setPrefix() {
    this.prefix = wasFound() ? SqlUtil.getAlias(this.parts.length - 1, this.parts) : table;
  }

  public TableSchema.SchemaDefinition[] getSchemaFields() {
    return Objects.nonNull(this.schema)
        ? this.schema.getFields()
        : new TableSchema.SchemaDefinition[0];
  }

  public Stream<String> getPartsStream() {
    return Arrays.stream(getParts());
  }
  // endregion
}
