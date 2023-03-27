package bio.terra.cda.app.builders;

import bio.terra.cda.app.helpers.StorageServiceHelper;
import bio.terra.cda.app.helpers.TableSchemaHelper;
import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.QueryField;
import java.io.IOException;

import bio.terra.cda.app.models.SchemaDefinition;
import bio.terra.cda.app.models.TableDefinition;
import bio.terra.cda.app.service.StorageService;
import bio.terra.cda.app.util.TableSchema;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class ParameterBuilderTest {

  @Test
  void testAddParameterDuplicates() throws IOException {
    ParameterBuilder builder = new ParameterBuilder();
    TableSchema tableSchema = TableSchemaHelper.getNewTableSchema("v3");
    DataSetInfo dataSetInfo = tableSchema.getDataSetInfo("v3");
    QueryField queryField =
        new QueryField(
            "subject_id",
            "subject_id",
            new String[] {"id"},
            "subject_id",
            "subject_id",
            "Subject",
            "",
            dataSetInfo.getSchemaDefinitionByFieldName("subject_id"),
            false,
            false);

    builder.addParameterValue(queryField, "test");
    builder.addParameterValue(queryField, "test2");
    assert builder.getParameterValueMap().containsKey("subject_id_1");
    assert builder.getParameterValueMap().containsKey("subject_id_2");
  }
}
