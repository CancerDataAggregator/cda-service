package bio.terra.cda.app.util;

public class NestedColumn {
  private String column;
  private String unnestClause;

  public NestedColumn() {}

  public NestedColumn(String column, String unnestClause) {
    this.column = column;
    this.unnestClause = unnestClause;
  }

  public NestedColumn generate(String qualifiedColumnName) {
    return new Builder().generate(qualifiedColumnName);
  }

  public static class Builder {
    /**
     * column -> SELECT DISTINCT(column)
     *
     * <p>D.column -> SELECT DISTINCT(_D.column) FROM TABLE, UNNEST(D) AS _D
     *
     * <p>A.B.C.D.column -> > SELECT DISTINCT(_D.column) FROM TABLE, UNNEST(A) AS _A, UNNEST(_A.B)
     * AS _B, UNNEST(_B.C) AS _C, UNNEST(_C.D) AS _D
     */
    public NestedColumn generate(String qualifiedColumnName) throws IllegalArgumentException {
      String unnestClause = null;
      String newColumn = null;
      if (qualifiedColumnName != null) {
        String[] c = qualifiedColumnName.split("\\.");
        if (c.length > 1) {
          newColumn = "_" + c[c.length - 2] + "." + c[c.length - 1];
          unnestClause = ", UNNEST(" + c[0] + ") AS _" + c[0];
          for (int n = 1; n < c.length - 1; n++) {
            unnestClause += ", UNNEST(_" + c[n - 1] + "." + c[n] + ") AS _" + c[n];
          }
          return new NestedColumn(newColumn, unnestClause);
        } else if (c.length > 0) {
          return new NestedColumn(qualifiedColumnName, "");
        }
      }
      throw new IllegalArgumentException("");
    }
  }

  public String getColumn() {
    return column;
  }

  public void setColumn(String column) {
    this.column = column;
  }

  public String getUnnestClause() {
    return unnestClause;
  }

  public void setUnnestClause(String unnestClause) {
    this.unnestClause = unnestClause;
  }
}
