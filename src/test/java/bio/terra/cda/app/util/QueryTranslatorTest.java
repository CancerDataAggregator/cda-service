package bio.terra.cda.app.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.jupiter.api.Test;

class QueryTranslatorTest {

  static final Path TEST_FILES = Paths.get("src/test/resources/query");

  public static final String TABLE = "TABLE";
  public static final String QUALIFIED_TABLE = "GROUP." + TABLE;

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testQuerySimple() throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve("query1.json"));

    String expectedSql =
        String.format(
            "SELECT %2$s.* FROM %1$s AS %2$s WHERE (%2$s.project_id = 'TCGA-OV')",
            QUALIFIED_TABLE, TABLE);

    Query query = objectMapper.readValue(jsonQuery, Query.class);
    String translatedQuery = QueryTranslator.sql(QUALIFIED_TABLE, query);

    assertEquals(expectedSql, translatedQuery);
  }

  @Test
  public void testQueryComplex() throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve("query2.json"));

    String EXPECTED_SQL =
        String.format(
            "SELECT %2$s.* FROM %1$s AS %2$s, UNNEST(demographic) AS _demographic, UNNEST(project) AS _project, "
                + "UNNEST(diagnoses) AS _diagnoses WHERE (((_demographic.age_at_index >= 50) AND "
                + "(_project.project_id = 'TCGA-OV')) AND (_diagnoses.figo_stage = 'Stage IIIC'))",
            QUALIFIED_TABLE, TABLE);

    Query query = objectMapper.readValue(jsonQuery, Query.class);
    String translatedQuery = QueryTranslator.sql(QUALIFIED_TABLE, query);

    assertEquals(EXPECTED_SQL, translatedQuery);
  }

  @Test
  public void testQueryNested() throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve("query3.json"));

    String expectedSql =
        String.format(
            "SELECT %2$s.* FROM %1$s AS %2$s, UNNEST(A) AS _A, UNNEST(_A.B) AS _B, "
                + "UNNEST(_B.C) AS _C, UNNEST(_C.D) AS _D WHERE (_D.column = value)",
            QUALIFIED_TABLE, TABLE);

    Query query = objectMapper.readValue(jsonQuery, Query.class);
    String translatedQuery = QueryTranslator.sql(QUALIFIED_TABLE, query);

    assertEquals(expectedSql, translatedQuery);
  }

  @Test
  public void testQueryFrom() throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve("query-subquery.json"));

    String expectedSql =
        String.format(
            "SELECT %2$s.* FROM "
                + "(SELECT %2$s.* FROM %1$s AS %2$s, UNNEST(ResearchSubject) AS _ResearchSubject, "
                + "UNNEST(_ResearchSubject.identifier) AS _identifier "
                + "WHERE (_identifier.system = 'PDC')) AS %2$s,"
                + " UNNEST(ResearchSubject) AS _ResearchSubject, "
                + "UNNEST(_ResearchSubject.identifier) AS _identifier WHERE (_identifier.system = 'GDC')",
            QUALIFIED_TABLE, TABLE);

    Query query = objectMapper.readValue(jsonQuery, Query.class);
    String translatedQuery = QueryTranslator.sql(QUALIFIED_TABLE, query);

    assertEquals(expectedSql, translatedQuery);
  }

  @Test
  public void testQueryNot() throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve("query-not.json"));

    String expectedSql =
        String.format(
            "SELECT %2$s.* FROM %1$s AS %2$s, UNNEST(A) AS _A WHERE (NOT (1 = _A.B))",
            QUALIFIED_TABLE, TABLE);

    Query query = objectMapper.readValue(jsonQuery, Query.class);
    String translatedQuery = QueryTranslator.sql(QUALIFIED_TABLE, query);

    assertEquals(expectedSql, translatedQuery);
  }

  @Test
  public void testQueryAmbiguous() throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve("query-ambiguous.json"));

    String expectedSql =
        String.format(
            "SELECT %2$s.* FROM (SELECT %2$s.* FROM %1$s AS %2$s WHERE (%2$s.id = 'that')) AS %2$s WHERE (%2$s.id = 'this')",
            QUALIFIED_TABLE, TABLE);

    Query query = objectMapper.readValue(jsonQuery, Query.class);
    String translatedQuery = QueryTranslator.sql(QUALIFIED_TABLE, query);

    assertEquals(expectedSql, translatedQuery);
  }

  @Test
  void parseColumnNameTest() throws Exception {
    String tableColumn = "ICD_Coding.ICD_0_3_Coding.Site_recode";
    Map<String, String> parts = QueryTranslator.parseTableName(tableColumn);
    assertEquals(parts.get("tableName"), "ICD_Coding.ICD_0_3_Coding");
    assertEquals(parts.get("columnName"), "Site_recode");
  }
}
