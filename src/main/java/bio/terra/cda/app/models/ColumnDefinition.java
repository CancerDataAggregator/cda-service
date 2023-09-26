package bio.terra.cda.app.models;

import java.util.ArrayList;
import java.util.List;

public class ColumnDefinition {
  private Boolean isNullable;
  private String name;

  private String tableName;
  private String type;
  private String description;
//  private List<ForeignKey> foreignKeys = new ArrayList<>();
  private String alias;
  private CountByField[] countByFields;
  private boolean excludeFromSelect;

  public ColumnDefinition(String name, String tableName, String type, String description, Boolean isNullable) {
    setName(name.toLowerCase());
    setTableName(tableName);
    setType(type);
    setDescription(description);
    setNullable(isNullable);
  }

  public Boolean isNullable() {
    return isNullable;
  }

  public void setNullable(Boolean nullable) {
    this.isNullable = nullable;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }


  public void setDescription(String description) {
    this.description = description;
  }

  public String getDescription() {
    return this.description;
  }


  public String getAlias() {
    if (alias == null) {
      return name;
    }
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public CountByField[] getCountByFields() {
    return countByFields;
  }

  public void setCountByFields(CountByField[] countByFields) {
    this.countByFields = countByFields;
  }

  public boolean isExcludeFromSelect() {
    return excludeFromSelect;
  }

  public void setExcludeFromSelect(boolean excludeFromSelect) {
    this.excludeFromSelect = excludeFromSelect;
  }
}
