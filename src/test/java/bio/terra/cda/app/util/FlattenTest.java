package bio.terra.cda.app.util;

import bio.terra.cda.app.models.Subject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.fasterxml.jackson.databind.type.MapType;
import org.junit.jupiter.api.Test;
import org.springframework.vault.support.JsonMapFlattener;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

class FlattenTest {

  static final Path TEST_FILES = Paths.get("src/test/resources/json_files");

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testSubjectMapping() throws Exception {
    String jsonString = Files.readString(TEST_FILES.resolve("tcga_brca1.json"));
    Subject subject = objectMapper.readValue(jsonString, Subject.class);
    System.out.println(subject);
  }

  @Test
  public void testFlatten() throws Exception {
    String data = Files.readString(TEST_FILES.resolve("tcga_brca1.json"));
    // Json string to Map<String, Object>
    final MapType type =
        objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class);
    final Map<String, Object> map = objectMapper.readValue(data, type);

    // Using springframework.vault flatten method
    Map<String, String> keyMap = JsonMapFlattener.flattenToStringMap(map);

    System.out.println(keyMap);
  }

  /**
   * I think this one is the best because it doesn't rely on Object Mapping and simply takes Generic JSON
   * and unwinds it in an understandable manner.
   * @throws Exception
   */
  @Test
  public void testFlattenGenericMapping() throws Exception {
    String jsonString = Files.readString(TEST_FILES.resolve("TCGA-EW-A1J2.json"));
    JsonNode actualObj = objectMapper.readTree(jsonString);
    Map<String, ValueNode> m = FlattenJsonUtil.flattenJson(actualObj);

    for (Map.Entry<String, ValueNode> kv : m.entrySet()) {
      System.out.println(kv.getKey() + "=" + kv.getValue().asText());
    }
  }
}
