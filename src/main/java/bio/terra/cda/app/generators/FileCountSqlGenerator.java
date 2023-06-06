package bio.terra.cda.app.generators;

import bio.terra.cda.app.models.ColumnDefinition;
import bio.terra.cda.app.models.RdbmsSchema;
import bio.terra.cda.generated.model.Query;

import java.util.Optional;

@CountQueryGenerator(
    entity = "file",
    totalFieldsToCount = {
        "id",
    },
    groupedFieldsToCount = {
        "data_category",
        "data_type",
        "file_identifier_system",
        "file_format"
    })
public class FileCountSqlGenerator extends EntityCountSqlGenerator {
  protected Optional<String> secondaryTable = Optional.empty();

  public FileCountSqlGenerator(Query rootQuery) {
    super(rootQuery, true);
  }

  public FileCountSqlGenerator(Query rootQuery, Optional<String> entityTablename) {
    super(rootQuery, true);
    secondaryTable = entityTablename;
  }

  @Override
  protected ColumnDefinition getSecondaryEntity() {
    if (secondaryTable.isPresent()) {
      return dataSetInfo.getColumnDefinitionByFieldName( "id", secondaryTable.get());
    }
    return null;
  }
}
