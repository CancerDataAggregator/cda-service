package bio.terra.cda.app.util;

public class NestedColumn {
  private final String column;
  private final String unnestClause;

  public NestedColumn(String column, String unnestClause) {
    this.column = column;
    this.unnestClause = unnestClause;
  }

  public static NestedColumn generate(String qualifiedColumnName) {
    return Builder.generate(qualifiedColumnName);
  }

  public static class Builder {
    /*
     * column -> SELECT DISTINCT(column)
     * D.column -> SELECT DISTINCT(_D.column) FROM TABLE, UNNEST(D) AS _D
     * A.B.C.D.column -> > SELECT DISTINCT(_D.column) FROM TABLE, UNNEST(A) AS _A, UNNEST(_A.B)
     * AS _B, UNNEST(_B.C) AS _C, UNNEST(_C.D) AS _D
     */
    public static NestedColumn generate(String qualifiedColumnName)
        throws IllegalArgumentException {
      StringBuilder unnestClause = new StringBuilder();
      String newColumn = null;
      if (qualifiedColumnName != null) {
        String[] c = qualifiedColumnName.split("\\.");
        if (c.length > 1) {
          newColumn = "_" + c[c.length - 2] + "." + c[c.length - 1];
          unnestClause.append(", UNNEST(" + c[0] + ") AS _" + c[0]);
          for (int n = 1; n < c.length - 1; n++) {
            unnestClause.append(", UNNEST(_" + c[n - 1] + "." + c[n] + ") AS _" + c[n]);
          }
          return new NestedColumn(newColumn, unnestClause.toString());
        }
        if (c.length == 1) {
          return new NestedColumn(qualifiedColumnName, "");
        }
      }
      // Case where a null or empty value is passed.
      throw new IllegalArgumentException("Empty column name");
    }
  }

  public String getColumn() {
    return column;
  }

  public String getUnnestClause() {
    return unnestClause;
  }
}
