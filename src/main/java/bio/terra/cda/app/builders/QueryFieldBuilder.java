package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import com.google.cloud.bigquery.Field;

import java.util.Map;

public class QueryFieldBuilder {
    private final Map<String, TableSchema.SchemaDefinition> baseSchema;
    private final Map<String, TableSchema.SchemaDefinition> fileSchema;
    private static final String FILE_MATCH = String.format("%s.", TableSchema.FILE_PREFIX.toLowerCase());
    private final String table;
    private final String fileTable;

    public QueryFieldBuilder(
            Map<String, TableSchema.SchemaDefinition> baseSchema,
            Map<String, TableSchema.SchemaDefinition> fileSchema,
            String table,
            String fileTable) {
        this.baseSchema = baseSchema;
        this.fileSchema = fileSchema;
        this.table = table;
        this.fileTable = fileTable;
    }

    public QueryField fromPath(String path) {
        boolean fileField = path.toLowerCase().startsWith(FILE_MATCH);

        String realPath = fileField ? path.substring(path.indexOf(".") + 1) : path;
        String[] parts = SqlUtil.getParts(realPath);
        TableSchema.SchemaDefinition schemaDefinition =
                (fileField ? this.fileSchema : this.baseSchema)
                        .get(realPath);
        String alias = SqlUtil.getAlias(parts.length - 1, parts);
        String columnText = getColumnText(schemaDefinition, parts, alias, fileField);

        return new QueryField(
                schemaDefinition.getName(),
                realPath,
                parts,
                alias,
                columnText,
                fileField,
                schemaDefinition);
    }

    protected String getColumnText(TableSchema.SchemaDefinition schemaDefinition, String[] parts, String alias, Boolean fileField) {
        String mode = schemaDefinition.getMode();

        if (mode.equals(Field.Mode.REPEATED.toString())) {
            return alias;
        } else if (parts.length == 1) {
            return String.format(SqlUtil.ALIAS_FIELD_FORMAT, fileField ? fileTable : table, schemaDefinition.getName());
        } else {
            return String.format(
                    SqlUtil.ALIAS_FIELD_FORMAT, SqlUtil.getAlias(parts.length - 2, parts), schemaDefinition.getName());
        }
    }
}
