package bio.terra.cda.app.model;

import java.util.List;

public class SchemaObjectList {
  private List<SchemaObject> result;
  private String query_sql;
  private int total_row_count;
  private String next_url;

  public List<SchemaObject> getResult() {
    return result;
  }

  public void setResult(List<SchemaObject> result) {
    this.result = result;
  }

  public String getQuery_sql() {
    return query_sql;
  }

  public void setQuery_sql(String query_sql) {
    this.query_sql = query_sql;
  }

  public int getTotal_row_count() {
    return total_row_count;
  }

  public void setTotal_row_count(int total_row_count) {
    this.total_row_count = total_row_count;
  }

  public String getNext_url() {
    return next_url;
  }

  public void setNext_url(String next_url) {
    this.next_url = next_url;
  }

  @Override
  public String toString() {
    return "SchemaObjectList{"
        + "result="
        + result
        + ", query_sql='"
        + query_sql
        + '\''
        + ", total_row_count="
        + total_row_count
        + ", next_url='"
        + next_url
        + '\''
        + '}';
  }
}
