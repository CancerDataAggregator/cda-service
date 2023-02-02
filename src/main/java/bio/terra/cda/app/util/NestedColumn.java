package bio.terra.cda.app.util;

import com.google.cloud.bigquery.Field;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.springframework.util.StringUtils;

public class NestedColumn {
  private final String column;
  private final Set<String> unnestClauses;

  public NestedColumn(String column, Set<String> unnestClauses) {
    this.column = column;
    this.unnestClauses = unnestClauses;
  }

  public static NestedColumn generate(
      String qualifiedColumnName, Map<String, TableSchema.SchemaDefinition> tableSchemaMap) {
    return Builder.generate(qualifiedColumnName, tableSchemaMap);
  }

  public static class Builder {
    /*
     * column -> SELECT DISTINCT(column)
     * D.column -> SELECT DISTINCT(_D.column) FROM TABLE, UNNEST(D) AS _D
     * A.B.C.D.column -> > SELECT DISTINCT(_D.column) FROM TABLE, UNNEST(A) AS _A, UNNEST(_A.B)
     * AS _B, UNNEST(_B.C) AS _C, UNNEST(_C.D) AS _D
     */
    public static NestedColumn generate(
        String qualifiedColumnName, Map<String, TableSchema.SchemaDefinition> tableSchemaMap)
        throws IllegalArgumentException {
      Set<String> unnestClauses = new LinkedHashSet<String>();
      String newColumn = null;
      if (StringUtils.hasLength(qualifiedColumnName)) {
        String[] c = SqlUtil.getParts(qualifiedColumnName);
        int modifier = 1;
        boolean repeated =
            tableSchemaMap
                .get(qualifiedColumnName)
                .getMode()
                .equals(Field.Mode.REPEATED.toString());

        if (c.length == 1 && !repeated) {
          return new NestedColumn(qualifiedColumnName, new LinkedHashSet<String>());
        }

        if (repeated) {
          modifier = 0;
        }

        if (c.length > modifier) {
          if (repeated) {
            newColumn = "_" + c[c.length - 1];
          } else {
            newColumn = "_" + c[c.length - 2] + "." + c[c.length - 1];
          }
          unnestClauses.add(", UNNEST(" + c[0] + ") AS _" + c[0]);
          for (int n = 1; n < c.length - modifier; n++) {
            unnestClauses.add(", UNNEST(_" + c[n - 1] + "." + c[n] + ") AS _" + c[n]);
          }
          return new NestedColumn(newColumn, unnestClauses);
        }
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
