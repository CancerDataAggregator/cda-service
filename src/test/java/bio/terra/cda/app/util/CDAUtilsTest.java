package bio.terra.cda.app.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class CDAUtilsTest {

  @Test
  void parseColumnNameTest() {
    String tableColumn = "ICD_Coding.ICD_0_3_Coding.Site_recode";
    Map<String, String> parts = CDAUtils.parseTableName(tableColumn);

    assertThat(parts.get("tableName"), equalTo("CD_Coding.ICD_0_3_Coding"));
    assertThat(parts.get("columnName"), equalTo("Site_recode"));
  }
}
