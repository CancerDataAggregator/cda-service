package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import com.google.cloud.bigquery.Field;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class QueryFieldBuilder {
  private final Map<String, TableSchema.SchemaDefinition> baseSchema;
  private final Map<String, TableSchema.SchemaDefinition> fileSchema;
  private static final String FILE_MATCH =
      String.format("%s.", TableSchema.FILE_PREFIX.toLowerCase());
  private final String table;
  private final String fileTable;
  private final boolean filesQuery;

  public QueryFieldBuilder(
      Map<String, TableSchema.SchemaDefinition> baseSchema,
      Map<String, TableSchema.SchemaDefinition> fileSchema,
      String table,
      String fileTable,
      boolean filesQuery) {
    this.baseSchema = baseSchema;
    this.fileSchema = fileSchema;
    this.table = table;
    this.fileTable = fileTable;
    this.filesQuery = filesQuery;
  }

  public QueryField fromPath(String path) {
    boolean fileField = path.toLowerCase().startsWith(FILE_MATCH);

    String realPath = fileField ? path.substring(path.indexOf(".") + 1) : path;
    String[] parts = SqlUtil.getParts(realPath);
    TableSchema.SchemaDefinition schemaDefinition =
        (fileField ? this.fileSchema : this.baseSchema).get(realPath);

    if (Objects.isNull(schemaDefinition)) {
      throw new IllegalArgumentException(
          String.format("Column %s does not exist on table %s", realPath, fileField ? this.fileTable : this.table));
    }

    String[] newParts = parts;
    if (fileField && parts.length > 1) {
      newParts = Stream.concat(
              Stream.of(TableSchema.FILE_PREFIX),
              Arrays.stream(parts)).toArray(String[]::new);
    }

    String alias = SqlUtil.getAlias(newParts.length - 1, newParts);
    String columnText = getColumnText(schemaDefinition, newParts, alias, fileField);

    return new QueryField(
        schemaDefinition.getName(),
        realPath,
        parts,
        alias,
        columnText,
        fileField,
        schemaDefinition,
        filesQuery);
  }

  protected String getColumnText(
      TableSchema.SchemaDefinition schemaDefinition,
      String[] parts,
      String alias,
      Boolean fileField) {
    String mode = schemaDefinition.getMode();

    if (mode.equals(Field.Mode.REPEATED.toString())) {
      return alias;
    } else if (parts.length == 1) {
      return String.format(
          SqlUtil.ALIAS_FIELD_FORMAT, fileField ? fileTable : table, schemaDefinition.getName());
    } else {
      return String.format(
          SqlUtil.ALIAS_FIELD_FORMAT,
          SqlUtil.getAlias(parts.length - 2, parts),
          schemaDefinition.getName());
    }
  }
}
