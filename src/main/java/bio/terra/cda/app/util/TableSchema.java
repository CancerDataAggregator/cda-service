package bio.terra.cda.app.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import org.springframework.core.io.ClassPathResource;

import javax.xml.validation.Schema;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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

    public static List<SchemaDefinition> getSchema(String version) throws IOException {
        return loadSchemaFromFile(getFileName(version));
    }

    public static Map<String, SchemaDefinition> buildSchemaMap(List<SchemaDefinition> definitions) throws IOException {
        Map<String, SchemaDefinition> definitionMap = new HashMap<String, SchemaDefinition>();
        addToMap("", definitions, definitionMap);
        return definitionMap;
    }

    public static List<SchemaDefinition> getSchemaByColumnName(List<SchemaDefinition> definitions, String columnName) {
        List<SchemaDefinition> newSchema = new ArrayList<SchemaDefinition>();

        definitions.forEach(def -> {
            hasColumn(def, columnName).ifPresent(newSchema::add);
        });

        return newSchema;
    }

    private static Optional<SchemaDefinition> hasColumn(SchemaDefinition definition, String columnName) {
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

        List<SchemaDefinition> newFields = new ArrayList<SchemaDefinition>();
        Arrays.stream(definition.getFields()).forEach(def -> {
            hasColumn(def, columnName).ifPresent(newFields::add);
        });

        if (newFields.size() == 0) {
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
        CollectionType collectionType = mapper.getTypeFactory().constructCollectionType(List.class, SchemaDefinition.class);

        return mapper.readValue(inputStream, collectionType);
    }

    private static Map<String, SchemaDefinition> getSchemaMappingFromFile(String fileName) throws IOException {
        List<SchemaDefinition> definitions = loadSchemaFromFile(fileName);

        Map<String, SchemaDefinition> definitionMap = new HashMap<String, SchemaDefinition>();
        addToMap("", definitions, definitionMap);
        return definitionMap;
    }

    private static void addToMap(String prefix, List<SchemaDefinition> definitions, Map<String, SchemaDefinition> definitionMap) {
        definitions.forEach(definition -> {
            var mapName = prefix.isEmpty() ? definition.name : String.format("%s.%s", prefix, definition.name);
            definitionMap.put(mapName, definition);
            if (definition.type.equals("RECORD")) {
                addToMap(mapName, List.of(definition.fields), definitionMap);
            }
        });
    }
}
