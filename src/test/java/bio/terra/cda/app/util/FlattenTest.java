package bio.terra.cda.app.util;

import bio.terra.cda.app.models.Subject;
import bio.terra.cda.app.models.Wrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

class FlattenTest {

  static final Path TEST_FILES = Paths.get("src/test/resources/json_files");

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testSubjectMapping() throws Exception {
    String jsonString = Files.readString(TEST_FILES.resolve("tcga_brca1.json"));
    Subject subject = objectMapper.readValue(jsonString, Subject.class);
    System.out.println(subject);
  }

  @Disabled
  public void testQueryComplex() throws Exception {
    String jsonString = Files.readString(TEST_FILES.resolve("tcga_brca1.json"));
    Wrapper wrapper = objectMapper.readValue(jsonString, Wrapper.class);
    System.out.println(wrapper);
  }

  @Disabled
  public void testQueryNested() throws Exception {
    String jsonQuery = Files.readString(TEST_FILES.resolve("TCGA-EW-A1J2.json"));
    System.out.println(jsonQuery);
  }
}
