package bio.terra.cda.app.flatten.model;

public class Row {

  public static String toSpreadsheetRow(Object[] cellData) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < cellData.length - 1; i++) {
      if (cellData[i] != null) {
        cellData[i].toString().replaceAll(",", "\\,");
        builder.append(cellData[i].toString() + "\t");
      } else {
        builder.append("\t");
      }
    }
    if (cellData[cellData.length - 1] != null) {
      builder.append(cellData[cellData.length - 1]);
    }

    return builder.toString();
  }
}
