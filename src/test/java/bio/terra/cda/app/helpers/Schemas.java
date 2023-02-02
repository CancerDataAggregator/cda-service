package bio.terra.cda.app.helpers;

import bio.terra.cda.app.models.SchemaDefinition;
import bio.terra.cda.app.models.TableDefinition;
import bio.terra.cda.app.util.TableSchema;
import java.io.IOException;
import java.util.Map;

public class Schemas {
  private final Map<String, SchemaDefinition> schemaMap;
  private final Map<String, SchemaDefinition> fileSchemaMap;
  private final TableDefinition schema;
  private final TableDefinition fileSchema;

  private Schemas(
      Map<String, SchemaDefinition> schemaMap,
      Map<String, SchemaDefinition> fileSchemaMap,
      TableDefinition schema,
      TableDefinition fileSchema) {
    this.schemaMap = schemaMap;
    this.fileSchemaMap = fileSchemaMap;
    this.schema = schema;
    this.fileSchema = fileSchema;
  }

  public Map<String, SchemaDefinition> getSchemaMap() {
    return schemaMap;
  }

  public Map<String, SchemaDefinition> getFileSchemaMap() {
    return fileSchemaMap;
  }

  public TableDefinition getSchema() {
    return schema;
  }

  public TableDefinition getFileSchema() {
    return fileSchema;
  }

  public static class SchemaBuilder {
    private final String table;
    private final String fileTable;

    public SchemaBuilder(String table, String fileTable) {
      this.table = table;
      this.fileTable = fileTable;
    }

    public Schemas build() throws IOException {
      var tableSchema = TableSchema.getSchema(this.table);
      var fileTableSchema = TableSchema.getSchema(this.fileTable);
      return new Schemas(
          TableSchema.buildSchemaMap(tableSchema),
          TableSchema.buildSchemaMap(fileTableSchema),
          tableSchema,
          fileTableSchema);
    }
  }
}
