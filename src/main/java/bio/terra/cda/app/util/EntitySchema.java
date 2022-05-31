package bio.terra.cda.app.util;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

public class EntitySchema {
    // region properties
    private String path;
    private TableSchema.SchemaDefinition schema;
    public static final String DEFAULT_PATH = "Subject";
    // endregion

    // region constructor
    public EntitySchema() {}
    // endregion

    // region getters and setters
    public String getPath(){
        if (Objects.isNull(this.path)) {
            return DEFAULT_PATH;
        }

        return this.path;
    }

    public EntitySchema setPath(String path) {
        this.path = path;
        return this;
    }

    public TableSchema.SchemaDefinition getSchema() { return this.schema; }

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
        return Objects.nonNull(path)
                ? SqlUtil.getParts(path)
                : new String[0];
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
