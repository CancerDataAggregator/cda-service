package bio.terra.cda.app.models;

public class OrderBy {
  private final String fieldName;
  private final String path;
  private final OrderByModifier modifier;

  private String collation = "COLLATE \"POSIX\"";

  public enum OrderByModifier {
    ASC("asc"),
    DESC("desc");

    private final String value;

    OrderByModifier(String value) {
      this.value = value;
    }
  }

  public OrderBy(String fieldName, String path, OrderByModifier modifier) {
    this.fieldName = fieldName;
    this.path = path;
    this.modifier = modifier;
  }

  public String getFieldName() {
    return this.fieldName;
  }

  public String getPath() {
    return this.path;
  }

  public OrderByModifier getModifier() {
    return this.modifier;
  }

  @Override
  public String toString() {
    return String.format("%s %s %s", path, collation, modifier.value);
  }
}
