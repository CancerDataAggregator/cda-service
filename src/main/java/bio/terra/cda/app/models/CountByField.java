package bio.terra.cda.app.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public class CountByField {
  private String field;
  private String table;
  private String alias;
  private CountByTypeEnum type;
  private TableInfo tableInfo;

  public enum CountByTypeEnum {
    TOTAL("TOTAL"),
    GROUPED("GROUPED");

    private String value;

    CountByTypeEnum(String value) {
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
    public static CountByField.CountByTypeEnum fromValue(String value) {
      for (CountByField.CountByTypeEnum b : CountByField.CountByTypeEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public CountByTypeEnum getType() {
    return type;
  }

  public void setType(CountByTypeEnum type) {
    this.type = type;
  }

  public TableInfo getTableInfo() {
    return tableInfo;
  }

  public void setTableInfo(TableInfo tableInfo) {
    this.tableInfo = tableInfo;
  }
}
