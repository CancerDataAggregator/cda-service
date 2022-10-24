package bio.terra.cda.app.models;

public class ColumnsReturn {
  private final String endpoint;
  private final String fieldName;
  private final String description;
  private final String type;
  private final String mode;

  public ColumnsReturn(
      String endpoint, String fieldName, String description, String type, String mode) {
    this.endpoint = endpoint;
    this.fieldName = fieldName;
    this.description = description;
    this.type = type;
    this.mode = mode;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getDescription() {
    return description;
  }

  public String getType() {
    return type;
  }

  public String getMode() {
    return mode;
  }
}
