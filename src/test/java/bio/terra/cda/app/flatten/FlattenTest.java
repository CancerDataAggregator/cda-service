package bio.terra.cda.app.flatten;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class FlattenTest {

  static final Path TEST_FILES = Paths.get("src/test/resources/json_files");

  @Test
  void testFlatteningUsingJsonFlattener() throws Exception {
    String jsonString = Files.readString(TEST_FILES.resolve("gdc_no_files1.json"));
    JsonFlattener jflat = new JsonFlattener();

    List<String> json2csv = jflat.json2Sheet(jsonString, "true").getJsonAsSpreadsheet();

    assertEquals(30, json2csv.size());
    assertTrue(
        jflat
            .json2Sheet(jsonString, "true")
            .getUniqueFields()
            .contains("member_of_research_project"));
  }

  @Disabled
  void testPathList() throws Exception {
    Configuration.setDefaults(
        new Configuration.Defaults() {
          private final JsonProvider jsonProvider = new JacksonJsonProvider();
          private final MappingProvider mappingProvider = new JacksonMappingProvider();

          // @Override
          public JsonProvider jsonProvider() {
            return jsonProvider;
          }

          // @Override
          public MappingProvider mappingProvider() {
            return mappingProvider;
          }

          // @Override
          public Set options() {
            return EnumSet.noneOf(Option.class);
          }
        });

    Configuration conf =
        Configuration.defaultConfiguration()
            .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
            .addOptions(Option.SUPPRESS_EXCEPTIONS);

    Configuration pathConf =
        Configuration.defaultConfiguration()
            .addOptions(Option.AS_PATH_LIST)
            .addOptions(Option.ALWAYS_RETURN_LIST);

    String jsonString = Files.readString(TEST_FILES.resolve("gdc_no_files1.json"));
    List<String> pathList = JsonPath.using(pathConf).parse(jsonString).read("$..*");

    for (String path : pathList) {
      System.out.println(pathList);
    }
  }

  @Test
  void testJson2SheetWriter() throws Exception {
    String jsonString = Files.readString(TEST_FILES.resolve("gdc_no_files1.json"));
    JsonFlattener jflat = new JsonFlattener().json2Sheet(jsonString, "true");
    List<Object[]> objects = jflat.getJsonAsSheet();
    assertEquals(30, objects.size());

    List<String> rows = jflat.getJsonAsSpreadsheet();
    assertEquals(30, rows.size());
  }

  // Move to integration tests
  @Disabled
  void testCsvWriter() throws Exception {
    String jsonString = Files.readString(TEST_FILES.resolve("gdc_no_files1.json"));
    JsonFlattener jflat = new JsonFlattener();

    List<String> json2csv = jflat.json2Sheet(jsonString, "true").getJsonAsSpreadsheet();
    jflat.json2Sheet(jsonString, "true").write2csv("./jsonOutput");
  }
}
