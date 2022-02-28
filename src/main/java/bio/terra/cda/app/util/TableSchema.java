package bio.terra.cda.app.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

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

    public static Map<String, SchemaDefinition> getFileTableSchema() throws IOException {
        return getJsonFromFile("schema/files-schema.json");
    }

    public static Map<String, SchemaDefinition> getSubjectTableSchema() throws IOException {
        return getJsonFromFile("schema/schema.json");
    }

    private static Map<String, SchemaDefinition> getJsonFromFile(String fileName) throws IOException {
        ClassPathResource resource = new ClassPathResource(fileName);
        InputStream inputStream = resource.getInputStream();
        ObjectMapper mapper = new ObjectMapper();
        CollectionType collectionType = mapper.getTypeFactory().constructCollectionType(List.class, SchemaDefinition.class);

        List<SchemaDefinition> definitions = mapper.readValue(inputStream, collectionType);

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
