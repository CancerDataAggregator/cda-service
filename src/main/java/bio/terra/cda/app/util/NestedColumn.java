package bio.terra.cda.app.util;

import java.util.LinkedHashSet;
import java.util.Set;

public class NestedColumn {
  private final String column;
  private final Set<String> unnestClauses;

  public NestedColumn(String column, Set<String> unnestClauses) {
    this.column = column;
    this.unnestClauses = unnestClauses;
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
      Set<String> unnestClauses = new LinkedHashSet<String>();
      String newColumn = null;
      if (qualifiedColumnName != null) {
        String[] c = qualifiedColumnName.split("\\.");
        if (c.length > 1) {
          newColumn = "_" + c[c.length - 2] + "." + c[c.length - 1];
          unnestClauses.add(", UNNEST(" + c[0] + ") AS _" + c[0]);
          for (int n = 1; n < c.length - 1; n++) {
            unnestClauses.add(", UNNEST(_" + c[n - 1] + "." + c[n] + ") AS _" + c[n]);
          }
          return new NestedColumn(newColumn, unnestClauses);
        }
        return new NestedColumn(qualifiedColumnName, new LinkedHashSet<String>());
      }
      // Case where a null or empty value is passed.
      throw new IllegalArgumentException("Empty column name");
    }
  }

  public String getColumn() {
    return column;
  }

  public Set<String> getUnnestClauses() {
    return unnestClauses;
  }
}
