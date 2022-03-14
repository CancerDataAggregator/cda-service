package bio.terra.cda.app.generators;

import bio.terra.cda.app.generators.SqlGenerator;
import bio.terra.cda.app.operators.QueryModule;
import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlGeneratorTest {

    static final Path TEST_FILES = Paths.get("src/test/resources/query");

    public static final String TABLE = "TABLE";
    public static final String QUALIFIED_TABLE = "GROUP." + TABLE;

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new QueryModule());

    @Test
    public void testQuerySimple() throws Exception {
        String jsonQuery = Files.readString(TEST_FILES.resolve("query1.json"));

        String expectedSql =
                String.format(
                        "SELECT %2$s.* FROM %1$s AS %2$s WHERE (UPPER(%2$s.A) = UPPER('value'))",
                        QUALIFIED_TABLE, TABLE);

        Query query = objectMapper.readValue(jsonQuery, Query.class);
        String translatedQuery = new SqlGenerator(QUALIFIED_TABLE, query, TABLE).generate();

        assertEquals(expectedSql, translatedQuery);
    }

    @Test
    public void testQueryComplex() throws Exception {
        String jsonQuery = Files.readString(TEST_FILES.resolve("query2.json"));

        String EXPECTED_SQL =
                String.format(
                        "SELECT %2$s.* FROM %1$s AS %2$s, UNNEST(%2$s.B) AS _B, UNNEST(_B.BB) AS _B_BB, "
                                + "UNNEST(%2$s.A1) AS _A1 WHERE (((_B.BA >= 50) AND "
                                + "(UPPER(_B_BB.BBB) = UPPER('value'))) AND (UPPER(_A1.A1A) = UPPER('value')))",
                        QUALIFIED_TABLE, TABLE);

        Query query = objectMapper.readValue(jsonQuery, Query.class);
        String translatedQuery = new SqlGenerator(QUALIFIED_TABLE, query, TABLE).generate();

        assertEquals(EXPECTED_SQL, translatedQuery);
    }

    @Test
    public void testQueryNested() throws Exception {
        String jsonQuery = Files.readString(TEST_FILES.resolve("query3.json"));

        String expectedSql =
                String.format(
                        "SELECT %2$s.* FROM %1$s AS %2$s, UNNEST(%2$s.B) AS _B, UNNEST(_B.BB) AS _B_BB, "
                                + "UNNEST(_B_BB.BBD) AS _B_BB_BBD, UNNEST(_B_BB_BBD.BBDD) AS _B_BB_BBD_BBDD WHERE (_B_BB_BBD_BBDD.BBDDE = 50)",
                        QUALIFIED_TABLE, TABLE);

        Query query = objectMapper.readValue(jsonQuery, Query.class);
        String translatedQuery = new SqlGenerator("GROUP.TABLE", query, "TABLE").generate();

        assertEquals(expectedSql, translatedQuery);
    }

    @Test
    public void testQueryFrom() throws Exception {
        String jsonQuery = Files.readString(TEST_FILES.resolve("query-subquery.json"));

        String expectedSql =
                String.format(
                        "SELECT %2$s.* FROM "
                                + "(SELECT %2$s.* FROM %1$s AS %2$s, UNNEST(%2$s.ResearchSubject) AS _ResearchSubject, "
                                + "UNNEST(_ResearchSubject.identifier) AS _ResearchSubject_identifier "
                                + "WHERE (UPPER(_ResearchSubject_identifier.system) = UPPER('PDC'))) AS %2$s,"
                                + " UNNEST(%2$s.ResearchSubject) AS _ResearchSubject, "
                                + "UNNEST(_ResearchSubject.identifier) AS _ResearchSubject_identifier WHERE (UPPER(_ResearchSubject_identifier.system) = UPPER('GDC'))",
                        "GROUP.all_v3_0_subjects_meta", "all_v3_0_subjects_meta");

        Query query = objectMapper.readValue(jsonQuery, Query.class);
        String translatedQuery = new SqlGenerator("GROUP.all_v3_0_subjects_meta", query, "all_v3_0_subjects_meta").generate();

        assertEquals(expectedSql, translatedQuery);
    }

    @Test
    public void testQueryNot() throws Exception {
        String jsonQuery = Files.readString(TEST_FILES.resolve("query-not.json"));

        String expectedSql =
                String.format(
                        "SELECT %2$s.* FROM %1$s AS %2$s, UNNEST(%2$s.A1) AS _A1 WHERE (NOT (1 = _A1.ANUM))",
                        QUALIFIED_TABLE, TABLE);

        Query query = objectMapper.readValue(jsonQuery, Query.class);
        String translatedQuery = new SqlGenerator(QUALIFIED_TABLE, query, TABLE).generate();

        assertEquals(expectedSql, translatedQuery);
    }

    @Test
    public void testQueryAmbiguous() throws Exception {
        String jsonQuery = Files.readString(TEST_FILES.resolve("query-ambiguous.json"));

        String expectedSql =
                String.format(
                        "SELECT %2$s.* FROM (SELECT %2$s.* FROM %1$s AS %2$s WHERE (UPPER(%2$s.A) = UPPER('that'))) AS %2$s WHERE (UPPER(%2$s.A) = UPPER('this'))",
                        QUALIFIED_TABLE, TABLE);

        Query query = objectMapper.readValue(jsonQuery, Query.class);
        String translatedQuery = new SqlGenerator(QUALIFIED_TABLE, query, TABLE).generate();

        assertEquals(expectedSql, translatedQuery);
    }
}
