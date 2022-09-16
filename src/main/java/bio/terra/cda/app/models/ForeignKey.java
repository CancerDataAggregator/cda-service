package bio.terra.cda.app.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public class ForeignKey {
  private String tableName;
  private String[] fields;
  private String tableAlias;

  public enum ForeignKeyTypeEnum {
    SINGLE("SINGLE"),
    COMPOSITE_OR("COMPOSITE_OR"),
    COMPOSITE_AND("COMPOSITE_AND");

    private String value;

    ForeignKeyTypeEnum(String value) {
      this.value = value;
    }

    @JsonValue
    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static ForeignKeyTypeEnum fromValue(String value) {
      for (ForeignKeyTypeEnum b : ForeignKeyTypeEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  private ForeignKeyTypeEnum type;

  public ForeignKeyTypeEnum getType() {
    return type;
  }

  public void setType(ForeignKeyTypeEnum type) {
    this.type = type;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getTableAlias() {
    return tableAlias;
  }

  public void setTableAlias(String tableAlias) {
    this.tableAlias = tableAlias;
  }

  public String[] getFields() {
    return fields;
  }

  public void setFields(String[] fields) {
    this.fields = fields;
  }
}
