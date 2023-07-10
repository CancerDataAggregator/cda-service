package bio.terra.cda.app.operators;

import static org.junit.jupiter.api.Assertions.assertEquals;

import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

public class OperatorDeserializerTest {
  static final Path TEST_FILES = Paths.get("src/test/resources/query");

  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new QueryModule());

  @Test
  void testKidney() throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve("query-kidney.json"));

    Query query = objectMapper.readValue(jsonQuery, Query.class);

    assertEquals(Query.class.getName(), query.getClass().getName());
    assertEquals(
        Column.class.getName(), query.getWhere().getL().getL().getL().getClass().getName());
    assertEquals(
        Quoted.class.getName(), query.getWhere().getL().getL().getR().getClass().getName());
  }

  @Test
  void testNot() throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve("query-not.json"));

    Query query = objectMapper.readValue(jsonQuery, Query.class);

    assertEquals(Not.class.getName(), query.getWhere().getClass().getName());
  }

  @Test
  void testInAndNotIn() throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve("query-in.json"));

    Query query = objectMapper.readValue(jsonQuery, Query.class);

    assertEquals(In.class.getName(), query.getWhere().getClass().getName());

    String notInQuery = Files.readString(TEST_FILES.resolve("query-notin.json"));

    Query notIn = objectMapper.readValue(notInQuery, Query.class);

    assertEquals(In.class.getName(), query.getWhere().getClass().getName());
  }
}
