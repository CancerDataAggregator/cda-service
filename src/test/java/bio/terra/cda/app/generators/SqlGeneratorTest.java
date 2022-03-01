package bio.terra.cda.app.generators;

import bio.terra.cda.app.generators.SqlGenerator;
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

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testQuerySimple() throws Exception {
        String jsonQuery = Files.readString(TEST_FILES.resolve("query1.json"));

        String expectedSql =
                String.format(
                        "SELECT %2$s.* FROM %1$s AS %2$s WHERE (UPPER(%2$s.project_id) = UPPER('TCGA-OV'))",
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
                        "SELECT %2$s.* FROM %1$s AS %2$s, UNNEST(demographic) AS _demographic, UNNEST(project) AS _project, "
                                + "UNNEST(diagnoses) AS _diagnoses WHERE (((_demographic.age_at_index >= 50) AND "
                                + "(UPPER(_project.project_id) = UPPER('TCGA-OV'))) AND (UPPER(_diagnoses.figo_stage) = UPPER('Stage IIIC')))",
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
                        "SELECT %2$s.* FROM %1$s AS %2$s, UNNEST(A) AS _A, UNNEST(_A.B) AS _B, "
                                + "UNNEST(_B.C) AS _C, UNNEST(_C.D) AS _D WHERE (UPPER(_D.column) = value)",
                        QUALIFIED_TABLE, TABLE);

        Query query = objectMapper.readValue(jsonQuery, Query.class);
        String translatedQuery = new SqlGenerator(QUALIFIED_TABLE, query, TABLE).generate();

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
                                + "WHERE (UPPER(_identifier.system) = UPPER('PDC'))) AS %2$s,"
                                + " UNNEST(ResearchSubject) AS _ResearchSubject, "
                                + "UNNEST(_ResearchSubject.identifier) AS _identifier WHERE (UPPER(_identifier.system) = UPPER('GDC'))",
                        QUALIFIED_TABLE, TABLE);

        Query query = objectMapper.readValue(jsonQuery, Query.class);
        String translatedQuery = new SqlGenerator(QUALIFIED_TABLE, query, TABLE).generate();

        assertEquals(expectedSql, translatedQuery);
    }

    @Test
    public void testQueryNot() throws Exception {
        String jsonQuery = Files.readString(TEST_FILES.resolve("query-not.json"));

        String expectedSql =
                String.format(
                        "SELECT %2$s.* FROM %1$s AS %2$s, UNNEST(A) AS _A WHERE (NOT (1 = UPPER(_A.B)))",
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
                        "SELECT %2$s.* FROM (SELECT %2$s.* FROM %1$s AS %2$s WHERE (UPPER(%2$s.id) = UPPER('that'))) AS %2$s WHERE (UPPER(%2$s.id) = UPPER('this'))",
                        QUALIFIED_TABLE, TABLE);

        Query query = objectMapper.readValue(jsonQuery, Query.class);
        String translatedQuery = new SqlGenerator(QUALIFIED_TABLE, query, TABLE).generate();

        assertEquals(expectedSql, translatedQuery);
    }
}
