package bio.terra.cda.app.builders;

import bio.terra.cda.app.helpers.Schemas;
import bio.terra.cda.app.models.QueryField;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class ParameterBuilderTest {

  @Test
  void testAddParameterDuplicates() throws IOException {
    Schemas schemas =
        new Schemas.SchemaBuilder("all_Subjects_v3_0_final", "all_Files_v3_0_final").build();
    ParameterBuilder builder =
        new ParameterBuilder(schemas.getSchemaMap(), schemas.getFileSchemaMap());
    QueryField queryField =
        new QueryField(
            "id", "id", new String[] {"id"}, "id", "id", false, schemas.getSchema().get(0), false);

    builder.addParameterValue(queryField, "test");
    builder.addParameterValue(queryField, "test2");
    assert builder.getParameterValueMap().containsKey("id_1");
    assert builder.getParameterValueMap().containsKey("id_2");
  }
}
