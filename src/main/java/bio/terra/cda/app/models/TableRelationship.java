package bio.terra.cda.app.models;

import java.util.*;

public class TableRelationship {
  private final String constraintName;
  private final String fromField;
  private final String fromTablename;
  private final String destinationTablename;
  private final String destinationField;
//  private final List<ForeignKey> foreignKeys;

  private TableRelationship(
      String constraintName,
      String fromTablename,
      String fromField,
      String destinationTablename,
      String getDestinationField)
//      List<ForeignKey> foreignKeys)
      {
    this.constraintName = constraintName;
    this.fromTablename = fromTablename;
    this.fromField = fromField;
    this.destinationTablename = destinationTablename;
    this.destinationField = getDestinationField;
//    this.foreignKeys = Objects.nonNull(foreignKeys) ? foreignKeys : new ArrayList<>();
  }

  public String getConstraintName() {
    return constraintName;
  }

  public String getFromTablename() {
    return fromTablename;
  }

  public String getDestinationTablename() {
    return destinationTablename;
  }

  public String getFromField() {
    return fromField;
  }


  public String getDestinationField() {
    return destinationField;
  }

//  public List<ForeignKey> getForeignKeys() {
//    return foreignKeys;
//  }
//
//  public TableRelationship addForeignKey(ForeignKey foreignKey) {
//    this.foreignKeys.add(foreignKey);
//    return this;
//  }

//  public static TableRelationship of(
//      String constraintName,
//      String fromTableInfo,
//      String field,
//      String destinationTableInfo,
//      String foreignKeyField) {
//    return new TableRelationship(
//        constraintName, fromTableInfo, field, destinationTableInfo, Collections.singletonList(ForeignKey.ofSingle(destinationTableInfo, foreignKeyField)));
//  }

  public static TableRelationship of(
      String constraintName,
      String fromTablename,
      String fromField,
      String destinationTablename,
      String destinationField) {
    return new TableRelationship(
        constraintName, fromTablename, fromField, destinationTablename, destinationField);
  }


}
