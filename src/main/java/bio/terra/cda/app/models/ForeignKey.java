package bio.terra.cda.app.models;

import java.util.Arrays;
import java.util.Objects;

public class ForeignKey {
  private String fromTableName;
  private String fromField;
  private String destinationTableName;
  private String[] fields;


  public static ForeignKey ofSingle(String fromTablename, String fromField, String destinationTableName, String foreignField) {
    ForeignKey fk = new ForeignKey();
    fk.setFromTableName(fromTablename);
    fk.setFromField(fromField);
    fk.setDestinationTableName(destinationTableName);
    fk.setFields(new String[]{foreignField});
    return fk;
  }

  public String getFromTableName() {
    return fromTableName;
  }

  public void setFromTableName(java.lang.String fromTableName) {
    this.fromTableName = fromTableName;
  }

  public String getDestinationTableName() {
    return destinationTableName;
  }

  public void setDestinationTableName(String tableName) {
    this.destinationTableName = tableName;
  }

  public String getFromField() {
    return fromField;
  }

  public void setFromField(String fromField) {
    this.fromField = fromField;
  }

  public String[] getFields() {
    return fields;
  }

  public void setFields(String[] fields) {
    this.fields = fields;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ForeignKey that = (ForeignKey) o;
    return Objects.equals(fromTableName, that.fromTableName) && Objects.equals(fromField, that.fromField) && Objects.equals(destinationTableName, that.destinationTableName) && Arrays.equals(fields, that.fields);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(fromTableName, fromField, destinationTableName);
    result = 31 * result + Arrays.hashCode(fields);
    return result;
  }
}
