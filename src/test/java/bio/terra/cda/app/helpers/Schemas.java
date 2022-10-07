package bio.terra.cda.app.helpers;

import bio.terra.cda.app.util.TableSchema;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Schemas {
  private final Map<String, TableSchema.SchemaDefinition> schemaMap;
  private final Map<String, TableSchema.SchemaDefinition> fileSchemaMap;
  private final TableSchema.TableDefinition schema;
  private final TableSchema.TableDefinition fileSchema;

  private Schemas(
      Map<String, TableSchema.SchemaDefinition> schemaMap,
      Map<String, TableSchema.SchemaDefinition> fileSchemaMap,
      TableSchema.TableDefinition schema,
      TableSchema.TableDefinition fileSchema) {
    this.schemaMap = schemaMap;
    this.fileSchemaMap = fileSchemaMap;
    this.schema = schema;
    this.fileSchema = fileSchema;
  }

  public Map<String, TableSchema.SchemaDefinition> getSchemaMap() {
    return schemaMap;
  }

  public Map<String, TableSchema.SchemaDefinition> getFileSchemaMap() {
    return fileSchemaMap;
  }

  public TableSchema.TableDefinition getSchema() {
    return schema;
  }

  public TableSchema.TableDefinition getFileSchema() {
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
