package bio.terra.cda.app.helpers;

import bio.terra.cda.app.operators.QueryModule;
import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class QueryFileReader {
  static final Path TEST_FILES = Paths.get("src/test/resources/query");

  private static final ObjectMapper objectMapper =
      new ObjectMapper().registerModule(new QueryModule());

  private QueryFileReader() {}

  public static Query getQueryFromFile(String fileName) throws IOException {
    String jsonQuery = Files.readString(TEST_FILES.resolve(fileName));

    return objectMapper.readValue(jsonQuery, Query.class);
  }
}
