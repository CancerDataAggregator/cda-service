package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.models.RdbmsSchema;
import org.junit.jupiter.api.Test;

public class ParameterBuilderTest {

  @Test
  void testAddParameterDuplicates() {
    DataSetInfo dataSetInfo = RdbmsSchema.getDataSetInfo();
    ParameterBuilder builder = new ParameterBuilder();
    QueryField queryField =
        new QueryField(
            "id",
            "id",
            "id",
            "table",
            "",
            dataSetInfo.getColumnDefinitionByFieldName("subject_id"),
            false,
            false);

    builder.addParameterValue(queryField, "test");
    builder.addParameterValue(queryField, "test2");
    assert builder.getParameterValueMap().hasValue("parameter_1");
    assert builder.getParameterValueMap().hasValue("parameter_2");
  }
}
