package bio.terra.cda.app.models;

import java.util.*;

public class TableRelationship {
  private final String field;

  private final Map<String, List<ForeignKey>> foreignKeyMap;
  private final TableRelationshipTypeEnum type;
  private final TableInfo fromTableInfo;
  private final TableInfo destinationTableInfo;
  private final List<ForeignKey> foreignKeys;
  private final boolean parent;
  private final boolean array;

  private TableRelationship(
      TableInfo fromTableInfo,
      String field,
      TableRelationshipTypeEnum type,
      TableInfo destinationTableInfo,
      boolean parent,
      List<ForeignKey> foreignKeys,
      boolean array) {
    this.fromTableInfo = fromTableInfo;
    this.field = field;
    this.type = type;
    this.destinationTableInfo = destinationTableInfo;
    this.parent = parent;
    this.foreignKeys = Objects.nonNull(foreignKeys) ? foreignKeys : new ArrayList<>();
    this.array = array;
    this.foreignKeyMap = new HashMap<>();
    this.foreignKeyMap.put(field, foreignKeys);
  }

  public TableRelationshipTypeEnum getType() {
    return type;
  }

  public TableInfo getFromTableInfo() {
    return fromTableInfo;
  }

  public TableInfo getDestinationTableInfo() {
    return destinationTableInfo;
  }

  public List<ForeignKey> getForeignKeys() {
    return foreignKeys;
  }

  public TableRelationship addForeignKey(String fieldName, ForeignKey foreignKey) {
    if (this.foreignKeyMap.containsKey(fieldName)) {
      this.foreignKeyMap.get(fieldName).add(foreignKey);
    } else {
      this.foreignKeyMap.put(fieldName, List.of(foreignKey));
    }
    return this;
  }

  public TableRelationship addForeignKeys(String fieldName, List<ForeignKey> foreignKeyList) {
    if (this.foreignKeyMap.containsKey(fieldName)) {
      this.foreignKeyMap.get(fieldName).addAll(foreignKeyList);
    } else {
      this.foreignKeyMap.put(fieldName, foreignKeyList);
    }
    return this;
  }

  public Map<String, List<ForeignKey>> getForeignKeyMap() {
    return this.foreignKeyMap;
  }

  public static TableRelationship of(
      TableInfo fromTableInfo,
      String field,
      TableRelationshipTypeEnum type,
      TableInfo destinationTableInfo) {
    return new TableRelationshipBuilder()
        .setField(field)
        .setType(type)
        .setFromTableInfo(fromTableInfo)
        .setDestinationTableInfo(destinationTableInfo)
        .build();
  }

  public static TableRelationship of(
      TableInfo fromTableInfo,
      String field,
      TableRelationshipTypeEnum type,
      TableInfo destinationTableInfo,
      boolean parent) {
    return new TableRelationshipBuilder()
        .setField(field)
        .setType(type)
        .setFromTableInfo(fromTableInfo)
        .setDestinationTableInfo(destinationTableInfo)
        .setParent(parent)
        .build();
  }

  public String getField() {
    return field;
  }

  public boolean isParent() {
    return parent;
  }

  public boolean isArray() {
    return array;
  }

  public enum TableRelationshipTypeEnum {
    UNNEST("UNNEST"),
    JOIN("JOIN");

    private String value;

    TableRelationshipTypeEnum(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }

  public static class TableRelationshipBuilder {
    private TableRelationshipTypeEnum type;
    private TableInfo fromTableInfo;
    private TableInfo destinationTableInfo;
    private String field;
    private List<ForeignKey> foreignKeys;
    private boolean parent;
    private boolean array;

    public TableRelationshipBuilder() {
      this.parent = false;
      this.array = false;
    }

    public TableRelationshipBuilder setType(TableRelationshipTypeEnum type) {
      this.type = type;
      return this;
    }

    public TableRelationshipBuilder setFromTableInfo(TableInfo tableInfo) {
      this.fromTableInfo = tableInfo;
      return this;
    }

    public TableRelationshipBuilder setDestinationTableInfo(TableInfo tableInfo) {
      this.destinationTableInfo = tableInfo;
      return this;
    }

    public TableInfo getDestinationTableInfo() {
      return this.destinationTableInfo;
    }

    public TableRelationshipBuilder setField(String field) {
      this.field = field;
      return this;
    }

    public TableRelationshipBuilder setParent(boolean parent) {
      this.parent = parent;
      return this;
    }

    public TableRelationshipBuilder setForeignKeys(Collection<ForeignKey> foreignKeys) {
      this.foreignKeys = new ArrayList<>(foreignKeys);
      return this;
    }

    public TableRelationshipBuilder setArray(boolean array) {
      this.array = array;
      return this;
    }

    public TableRelationship build() {
      return new TableRelationship(
          this.fromTableInfo,
          this.field,
          this.type,
          this.destinationTableInfo,
          this.parent,
          this.foreignKeys,
          this.array);
    }
  }
}
