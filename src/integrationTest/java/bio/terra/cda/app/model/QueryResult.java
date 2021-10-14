package bio.terra.cda.app.model;

import java.util.List;

public class QueryResult {
  private String query_id;
  private String query_sql;
  private String message;
  private String statusCode;
  private List<String> causes;

  public String getQuery_id() {
    return query_id;
  }

  public void setQuery_id(String query_id) {
    this.query_id = query_id;
  }

  public String getQuery_sql() {
    return query_sql;
  }

  public void setQuery_sql(String query_sql) {
    this.query_sql = query_sql;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getStatusCode() {
    return statusCode;
  }

  public void setStatusCode(String statusCode) {
    this.statusCode = statusCode;
  }

  public List<String> getCauses() {
    return causes;
  }

  public void setCauses(List<String> causes) {
    this.causes = causes;
  }

  @Override
  public String toString() {
    return "QueryResult{"
        + "query_id='"
        + query_id
        + '\''
        + ", query_sql='"
        + query_sql
        + '\''
        + '}';
  }
}
