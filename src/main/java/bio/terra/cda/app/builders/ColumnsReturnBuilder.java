package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.ColumnsReturn;
import java.util.Locale;

public class ColumnsReturnBuilder {
  private String endpoint;
  private String fieldName;
  private String description;
  private String type;
  private Boolean isNullable;

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

  public ColumnsReturnBuilder setNullable(Boolean nullable) {
    this.isNullable = nullable;
    return this;
  }

  public static ColumnsReturn of(
      String endpoint, String fieldName, String description, String type, Boolean nullable) {
    ColumnsReturnBuilder builder = new ColumnsReturnBuilder();
    builder.setEndpoint(endpoint);
    builder.setFieldName(fieldName);
    builder.setDescription(description);
    builder.setType(type);
    builder.setNullable(nullable);
    return builder.build();
  }

  public ColumnsReturn build() {
    return new ColumnsReturn(endpoint, fieldName, description, type, isNullable);
  }
}
