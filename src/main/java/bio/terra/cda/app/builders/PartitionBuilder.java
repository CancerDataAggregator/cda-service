package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.Partition;
import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class PartitionBuilder {
    private final String fileTable;
    private static final String ID_FORMAT = "%s.id";
    private static final String SYSTEM_FORMAT = "%s.system";

    public PartitionBuilder(String fileTable) {
        this.fileTable = fileTable;
    }

    public Partition of(String path, String text) {
        return new Partition(path, text);
    }

    public Stream<Partition> fromParts(String[] parts, Map<String, TableSchema.SchemaDefinition> schemaMap) {
        return IntStream.range(0, parts.length)
                .mapToObj(i -> {
                    String path = SqlUtil.getPathFromParts(i, parts);
                    String alias = SqlUtil.getAlias(i, parts);

                    TableSchema.SchemaDefinition schemaDefinition = schemaMap.get(path);

                    if (parts[i].equals(TableSchema.IDENTIFIER_COLUMN)) {
                        return new Partition(path, String.format(SYSTEM_FORMAT, alias));
                    } else if (schemaDefinition.getMode().equals(Field.Mode.REPEATED.toString())
                        && !schemaDefinition.getType().equals(LegacySQLTypeName.RECORD.toString())){
                        return new Partition(path, SqlUtil.getAlias(i, parts));
                    }
                    return new Partition(
                            SqlUtil.getPathFromParts(i, parts),
                            String.format(ID_FORMAT, SqlUtil.getAlias(i, parts)));
                });
    }

    public Partition fromQueryField(QueryField queryField) {
        String[] parts = queryField.getParts();
        if (Arrays.asList(parts).contains(TableSchema.IDENTIFIER_COLUMN)
            && !parts[parts.length - 1].equals(TableSchema.IDENTIFIER_COLUMN)) {
            return new Partition(
                    queryField.getPath(),
                    String.format(SYSTEM_FORMAT,
                            SqlUtil.getAlias(parts.length - 2, parts)));
        } else {
            return new Partition(
                    queryField.getPath(),
                    String.format(ID_FORMAT,
                            queryField.isFileField()
                                ? fileTable
                                : SqlUtil.getAlias(parts.length - 2, parts)));
        }
    }
}
