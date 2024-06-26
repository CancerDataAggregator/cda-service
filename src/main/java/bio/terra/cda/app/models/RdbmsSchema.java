package bio.terra.cda.app.models;

import bio.terra.cda.app.models.DataSetInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;


public class RdbmsSchema {

    private static final ObjectMapper mapper = new ObjectMapper();

    private static DataSetInfo dataSetInfo;
    public static final String FILE_TABLE = "file";

    //TODO get this from app conifg
    private static String schema_file = "schema/cda_schema.json";
    public static JsonNode loadDbSchema(String fileName) throws IOException {
        ClassPathResource resource = new ClassPathResource(fileName);
        InputStream inputStream = resource.getInputStream();
        return mapper.readTree(inputStream);
    }

    public static void createDataSetInfo() {
        try {
            JsonNode node = loadDbSchema(schema_file);
            DataSetInfo.DataSetInfoBuilder dataSetInfoBuilder = new DataSetInfo.DataSetInfoBuilder();
            dataSetInfo = dataSetInfoBuilder.setDbSchema(node).build();
        } catch (IOException ex) {
            throw new RuntimeException("Could not read schema file", ex);
        }
    }

    public static DataSetInfo getDataSetInfo() {
        if (dataSetInfo == null) {
            createDataSetInfo();
        }
        return dataSetInfo;
    }

}
