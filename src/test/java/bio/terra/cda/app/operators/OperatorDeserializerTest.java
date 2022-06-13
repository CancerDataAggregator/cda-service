package bio.terra.cda.app.operators;

import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OperatorDeserializerTest {
    static final Path TEST_FILES = Paths.get("src/test/resources/query");

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new QueryModule());

    @Test
    void testKidney() throws Exception {
        String jsonQuery = Files.readString(TEST_FILES.resolve("query-kidney.json"));

        Query query = objectMapper.readValue(jsonQuery, Query.class);

        assertEquals(BasicOperator.class.getName(), query.getClass().getName());
        assertEquals(Column.class.getName(), ((BasicOperator)query).getL().getL().getL().getClass().getName());
        assertEquals(Quoted.class.getName(), ((BasicOperator)query).getL().getL().getR().getClass().getName());
    }

    @Test
    void testNot() throws Exception {
        String jsonQuery = Files.readString(TEST_FILES.resolve("query-not.json"));

        Query query = objectMapper.readValue(jsonQuery, Query.class);

        assertEquals(Not.class.getName(), query.getClass().getName());
    }

    @Test
    void testInAndNotIn() throws Exception {
        String jsonQuery = Files.readString(TEST_FILES.resolve("query-in.json"));

        Query query = objectMapper.readValue(jsonQuery, Query.class);

        assertEquals(In.class.getName(), query.getClass().getName());

        String notInQuery = Files.readString(TEST_FILES.resolve("query-notin.json"));

        Query notIn = objectMapper.readValue(notInQuery, Query.class);

        assertEquals(In.class.getName(), query.getClass().getName());
    }
}
