package bio.terra.cda.app.generators;

import bio.terra.cda.app.builders.JoinBuilder;
import bio.terra.cda.app.models.ColumnDefinition;
import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.RdbmsSchema;
import bio.terra.cda.generated.model.Query;

import java.util.LinkedHashMap;
import java.util.Map;

@EntityGeneratorData(entity = "file", hasFiles = true, defaultOrderBy = "file_id",
    aggregatedFields = {"file_identifier_system", "file_associated_project_associated_project"},
    aggregatedFieldsSelectString = {
        "json_agg(distinct (file_identifier.system, file_identifier.field_name, file_identifier.value)::system_data) as file_identifier",
        "json_agg(distinct file_associated_project.associated_project) AS file_associated_project"})
public class FileSqlGenerator extends EntitySqlGenerator {


  public FileSqlGenerator(Query rootQuery) {
    super(rootQuery, true);
  }

  public static Map<ColumnDefinition, String> getExternalFieldsAndSqlString() {
    Map<ColumnDefinition, String> newmap = new LinkedHashMap<>();
    DataSetInfo dsinfo = RdbmsSchema.getDataSetInfo();
    newmap.put(dsinfo.getColumnDefinitionByFieldName("file_identifier_system"),
        "json_agg(distinct (file_identifier.system, file_identifier.field_name, file_identifier.value)::system_data) as file_identifier");
    newmap.put(dsinfo.getColumnDefinitionByFieldName("file_associated_project_associated_project"),
        "json_agg(distinct file_associated_project.associated_project) AS file_associated_project");
    return newmap;
  }
}
