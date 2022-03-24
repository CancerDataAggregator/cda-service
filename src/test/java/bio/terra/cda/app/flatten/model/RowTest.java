package bio.terra.cda.app.flatten.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RowTest {

  static final String row_content =
      "TCGA-29-2435,Homo sapiens,female,white,not reported,-28779,Alive,,,8fa35df3-f544-4c47-bdd1-e4d6fc6662be,TCGA-OV";

  @Test
  public void testRowCreation() throws Exception {
    Object[] cells = row_content.split(",");
    String row = Row.toSpreadsheetRow(cells);
    assertTrue(row.contains("Homo sapiens"));
  }
}
