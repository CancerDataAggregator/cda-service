package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.ColumnsReturn;
import java.util.Locale;

public class ColumnsReturnBuilder {
  private String endpoint;
  private String fieldName;
  private String description;
  private String type;
  private String mode;

  public ColumnsReturnBuilder() {}

  public ColumnsReturnBuilder setFieldName(String fieldName) {
    this.fieldName = fieldName;
    return this;
  }

  public ColumnsReturnBuilder setEndpoint(String endpoint) {
    this.endpoint = endpoint.toLowerCase(Locale.ROOT);
    return this;
  }

  public ColumnsReturnBuilder setDescription(String description) {
    this.description = description;
    return this;
  }

  public ColumnsReturnBuilder setType(String type) {
    this.type = type;
    return this;
  }

  public ColumnsReturnBuilder setMode(String mode) {
    this.mode = mode;
    return this;
  }

  public ColumnsReturn of(
      String endpoint, String fieldName, String description, String type, String mode) {
    this.setEndpoint(endpoint);
    this.setFieldName(fieldName);
    this.setDescription(description);
    this.setType(type);
    this.setMode(mode);
    return build();
  }

  public ColumnsReturn build() {
    return new ColumnsReturn(endpoint, fieldName, description, type, mode);
  }
}
