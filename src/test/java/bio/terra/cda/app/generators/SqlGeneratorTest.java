package bio.terra.cda.app.generators;

import bio.terra.cda.app.generators.SqlGenerator;
import bio.terra.cda.app.operators.QueryModule;
import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlGeneratorTest {

    static final Path TEST_FILES = Paths.get("src/test/resources/query");

    public static final String TABLE = "TABLE";
    public static final String QUALIFIED_TABLE = "GROUP." + TABLE;

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new QueryModule());

    private static Stream<Arguments> queryData() {
        return Stream.of(
          Arguments.of("query1.json", QUALIFIED_TABLE, TABLE, "SELECT %2$s.* FROM %1$s AS %2$s WHERE (UPPER(%2$s.A) = UPPER('value'))"),
          Arguments.of("query2.json", QUALIFIED_TABLE, TABLE, "SELECT %2$s.* FROM %1$s AS %2$s, UNNEST(%2$s.B) AS _B, UNNEST(_B.BB) AS _B_BB, "
                  + "UNNEST(%2$s.A1) AS _A1 WHERE (((_B.BA >= 50) AND "
                  + "(UPPER(_B_BB.BBB) = UPPER('value'))) AND (UPPER(_A1.A1A) = UPPER('value')))"),
          Arguments.of("query3.json", QUALIFIED_TABLE, TABLE, "SELECT %2$s.* FROM %1$s AS %2$s, UNNEST(%2$s.B) AS _B, UNNEST(_B.BB) AS _B_BB, "
                  + "UNNEST(_B_BB.BBD) AS _B_BB_BBD, UNNEST(_B_BB_BBD.BBDD) AS _B_BB_BBD_BBDD WHERE (_B_BB_BBD_BBDD.BBDDE = 50)"),
          Arguments.of("query-subquery.json", "GROUP.all_v3_0_subjects_meta", "all_v3_0_subjects_meta", "SELECT %2$s.* FROM "
                  + "(SELECT %2$s.* FROM %1$s AS %2$s, UNNEST(%2$s.ResearchSubject) AS _ResearchSubject, "
                  + "UNNEST(_ResearchSubject.identifier) AS _ResearchSubject_identifier "
                  + "WHERE (UPPER(_ResearchSubject_identifier.system) = UPPER('PDC'))) AS %2$s,"
                  + " UNNEST(%2$s.ResearchSubject) AS _ResearchSubject, "
                  + "UNNEST(_ResearchSubject.identifier) AS _ResearchSubject_identifier WHERE (UPPER(_ResearchSubject_identifier.system) = UPPER('GDC'))"),
          Arguments.of("query-not.json", QUALIFIED_TABLE, TABLE, "SELECT %2$s.* FROM %1$s AS %2$s, UNNEST(%2$s.A1) AS _A1 WHERE (NOT (1 = _A1.ANUM))"),
          Arguments.of("query-ambiguous.json", QUALIFIED_TABLE, TABLE, "SELECT %2$s.* FROM (SELECT %2$s.* FROM %1$s AS %2$s WHERE (UPPER(%2$s.A) = UPPER('that'))) AS %2$s WHERE (UPPER(%2$s.A) = UPPER('this'))")
        );
    }

    @ParameterizedTest
    @MethodSource("queryData")
    void testQuery(String queryFile, String qualifiedTable, String table, String expectedQueryFormat) throws Exception {
        String jsonQuery = Files.readString(TEST_FILES.resolve(queryFile));

        String expectedSql = String.format(expectedQueryFormat, qualifiedTable, table);

        Query query = objectMapper.readValue(jsonQuery, Query.class);
        String translatedQuery = new SqlGenerator(qualifiedTable, query, table).generate();

        assertEquals(expectedSql, translatedQuery);
    }
}
