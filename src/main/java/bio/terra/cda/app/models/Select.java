package bio.terra.cda.app.models;

public class Select {
    private final String path;
    private final String alias;

    public Select(String path, String alias) {
        this.path = path;
        this.alias = alias;
    }

    public String getPath() { return this.path; }

    public String getAlias() { return this.alias; }

    @Override
    public String toString() {
        return String.format("%s AS %s", path, alias);
    }
}
