package bio.terra.cda.app.util;

public class QueryField {
    // region properties
    private String path;
    private String[] parts;
    private String alias;
    private String value;
    private Boolean fileField;
    private final String FILE_MATCH = String.format("%s.", TableSchema.FILE_PREFIX.toLowerCase());
    // endregion

    // region constructors
    public QueryField(String path) {
        setParts();
        setFileField();
    }
    // endregion

    // region getter and setters
    public String getPath() {
        return this.path;
    }

    public String[] getParts() {
        return this.parts;
    }

    public Boolean isFileField() {
        return this.fileField;
    }

    private void setFileField() {
        this.fileField = this.path.toLowerCase().startsWith(FILE_MATCH);
    }

    private void setParts() {
        this.parts = SqlUtil.getParts(this.path);
        setAlias();
    }

    public String getAlias() {
        return this.alias;
    }

    private void setAlias() {
        this.alias =  SqlUtil.getAlias(this.parts.length - 1, this.parts);
    }

    public String getValue() {
        return this.value;
    }

    public QueryField setValue(String value) {
        this.value = value;
        return this;
    }
    // endregion

    // region methods

    // endregion
}
