package bio.terra.cda.app.models;

public class OrderBy {

  private final ColumnDefinition col;
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

  public OrderBy(ColumnDefinition column, OrderByModifier modifier) {
    this.col = column;
    this.modifier = modifier;
  }

  public ColumnDefinition getColumnDefinition() {
    return col;
  }

  public String getFieldName() {
    return this.col.getName();
  }

  public String getPath() {
    return String.format("%s.%s", col.getTableName(), col.getName());
  }

  public OrderByModifier getModifier() {
    return this.modifier;
  }

  @Override
  public String toString() {
    return String.format("%s %s %s", getPath(), collation, modifier.value);
  }
}
