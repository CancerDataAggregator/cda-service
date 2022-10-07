package bio.terra.cda.app.util;

import bio.terra.cda.app.models.EntitySchema;
import bio.terra.cda.app.models.ForeignKey;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
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
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.core.io.ClassPathResource;

public class TableSchema {
  // region TableDefinition
  public static class TableDefinition {
    private String tableAlias;
    private SchemaDefinition[] definitions;

    public String getTableAlias() {
      return this.tableAlias;
    }

    public SchemaDefinition[] getDefinitions() {
      return definitions;
    }
  }
  // endregion

  // region SchemaDefinition
  public static class SchemaDefinition {
    private String mode;
    private String name;
    private String type;
    private String description;
    private SchemaDefinition[] fields;
    private ForeignKey[] foreignKeys;
    private Boolean partitionBy;
    private String alias;

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

    public ForeignKey[] getForeignKeys() {
      return foreignKeys;
    }

    public void setForeignKeys(ForeignKey[] foreignKeys) {
      this.foreignKeys = foreignKeys;
    }

    public Boolean getPartitionBy() {
      return !Objects.isNull(partitionBy) && partitionBy;
    }

    public void setPartitionBy(Boolean partitionBy) {
      this.partitionBy = partitionBy;
    }

    public String getAlias() {
      return alias;
    }

    public void setAlias(String alias) {
      this.alias = alias;
    }
  }
  // endregion

  public static final String FILE_PREFIX = "File";
  public static final String FILES_COLUMN = "Files";

  private TableSchema() {}

  public static TableDefinition getSchema(String version) throws IOException {
    return loadSchemaFromFile(getFileName(version));
  }

  public static Map<String, SchemaDefinition> buildSchemaMap(TableDefinition tableDefinition) {
    Map<String, SchemaDefinition> definitionMap = new HashMap<>();
    addToMap("", Arrays.asList(tableDefinition.getDefinitions()), definitionMap);
    return definitionMap;
  }

  private static String getFileName(String version) {
    return String.format("schema/%s.json", version);
  }

  private static TableDefinition loadSchemaFromFile(String fileName) throws IOException {
    ClassPathResource resource = new ClassPathResource(fileName);
    InputStream inputStream = resource.getInputStream();
    ObjectMapper mapper = new ObjectMapper();
    JavaType javaType =
        mapper.getTypeFactory().constructType(TableDefinition.class);

    return mapper.readValue(inputStream, javaType);
  }

  private static void addToMap(
      String prefix,
      List<SchemaDefinition> definitions,
      Map<String, SchemaDefinition> definitionMap) {
    definitions.forEach(
        definition -> {
          var mapName =
              prefix.isEmpty()
                  ? definition.name
                  : String.format(SqlUtil.ALIAS_FIELD_FORMAT, prefix, definition.name);
          definitionMap.put(mapName, definition);
          if (definition.type.equals(LegacySQLTypeName.RECORD.toString())) {
            addToMap(mapName, List.of(definition.fields), definitionMap);
          }
        });
  }
  // endregion
}
