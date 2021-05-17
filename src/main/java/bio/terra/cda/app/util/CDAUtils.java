package bio.terra.cda.app.util;

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CDAUtils {
  private static final Logger logger = LoggerFactory.getLogger(CDAUtils.class);

  /** Split the TABLE.ColumnName into component parts */
  public static Map<String, String> parseTableName(String qualifiedTable) {
    int dotPos = qualifiedTable.lastIndexOf('.');
    String table = dotPos == -1 ? qualifiedTable : qualifiedTable.substring(dotPos + 1);

    // List<String> parts = new LinkedList<String>(Arrays.asList(tableColumnString.split("\\.")));
    // int columnIndex = parts.size() - 1;
    // String column = parts.get(columnIndex); // last bit is the column
    parts.remove(columnIndex); // remove the column part
    String tableName = String.join(".", parts);

    return Map.of("columnName", column, "tableName", tableName);
  }
}
