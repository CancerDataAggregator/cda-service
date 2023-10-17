package bio.terra.cda.app.generators;

import bio.terra.cda.app.models.ColumnDefinition;
import bio.terra.cda.app.models.RdbmsSchema;
import bio.terra.cda.generated.model.Query;

import java.util.Arrays;
import java.util.Optional;


public class FileCountSqlGenerator {

  public static String[] getTotalFieldsToCount() {
    return Arrays.asList("file_id").toArray(new String[0]);
  }

  public static String[] getGroupedFieldsToCount() {
    return Arrays.asList(
        "data_category",
        "data_type",
        "file_identifier_system",
        "file_format").toArray(new String[0]);
  }
}
