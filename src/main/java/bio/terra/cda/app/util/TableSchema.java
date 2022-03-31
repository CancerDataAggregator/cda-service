package bio.terra.cda.app.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.core.io.ClassPathResource;

public class TableSchema {
  public static class SchemaDefinition {
    private String mode;
    private String name;
    private String type;
    private String description;
    private SchemaDefinition[] fields;

    public String getMode() {
      return mode;
    }

    public void setMode(String mode) {
      this.mode = mode;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public void setFields(SchemaDefinition[] fields) {
      this.fields = fields;
    }

    public SchemaDefinition[] getFields() {
      return this.fields;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public String getDescription() {
      return this.description;
    }
  }

  private TableSchema() {}

  public static List<SchemaDefinition> getSchema(String version) throws IOException {
    return loadSchemaFromFile(getFileName(version));
  }

  public static Map<String, SchemaDefinition> buildSchemaMap(List<SchemaDefinition> definitions) {
    Map<String, SchemaDefinition> definitionMap = new HashMap<>();
    addToMap("", definitions, definitionMap);
    return definitionMap;
  }

  public static List<SchemaDefinition> getSchemaByColumnName(
      List<SchemaDefinition> definitions, String columnName) {
    List<SchemaDefinition> newSchema = new ArrayList<>();

    definitions.forEach(def -> hasColumn(def, columnName).ifPresent(newSchema::add));

    return newSchema;
  }

  public static List<String> supportedSchemas() throws IOException {
    ClassLoader classLoader = TableSchema.class.getClassLoader();

    URL resource = classLoader.getResource("schema");

    if (resource == null) {
      throw new IOException("Schema does not exist");
    }

    try (Stream<Path> fileStream = Files.walk(Paths.get(resource.toURI()))) {
      return fileStream
          .filter(path -> path.getFileName().toString().endsWith(".json"))
          .map(
              path -> {
                var file = path.getFileName().toString();
                return file.substring(0, file.length() - 5).toLowerCase();
              })
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    }
  }

  private static Optional<SchemaDefinition> hasColumn(
      SchemaDefinition definition, String columnName) {
    SchemaDefinition newDef = new SchemaDefinition();
    newDef.setDescription(definition.getDescription());
    newDef.setMode(definition.getMode());
    newDef.setName(definition.getName());
    newDef.setType(definition.getType());

    if (newDef.getName().equals(columnName)) {
      return Optional.of(newDef);
    }

    if (definition.getFields() == null) {
      return Optional.empty();
    }

    List<SchemaDefinition> newFields = new ArrayList<>();
    Arrays.stream(definition.getFields())
        .forEach(def -> hasColumn(def, columnName).ifPresent(newFields::add));

    if (newFields.isEmpty()) {
      return Optional.empty();
    }

    SchemaDefinition[] fields = new SchemaDefinition[newFields.size()];
    newDef.setFields(newFields.toArray(fields));

    return Optional.of(newDef);
  }

  private static String getFileName(String version) {
    return String.format("schema/%s.json", version);
  }

  private static List<SchemaDefinition> loadSchemaFromFile(String fileName) throws IOException {
    ClassPathResource resource = new ClassPathResource(fileName);
    InputStream inputStream = resource.getInputStream();
    ObjectMapper mapper = new ObjectMapper();
    CollectionType collectionType =
        mapper.getTypeFactory().constructCollectionType(List.class, SchemaDefinition.class);

    return mapper.readValue(inputStream, collectionType);
  }

  private static void addToMap(
      String prefix,
      List<SchemaDefinition> definitions,
      Map<String, SchemaDefinition> definitionMap) {
    definitions.forEach(
        definition -> {
          var mapName =
              prefix.isEmpty() ? definition.name : String.format("%s.%s", prefix, definition.name);
          definitionMap.put(mapName, definition);
          if (definition.type.equals("RECORD")) {
            addToMap(mapName, List.of(definition.fields), definitionMap);
          }
        });
  }
}