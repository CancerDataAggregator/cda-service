package bio.terra.cda.app.helpers;

import bio.terra.cda.app.builders.ParameterBuilder;
import bio.terra.cda.app.builders.PartitionBuilder;
import bio.terra.cda.app.builders.QueryFieldBuilder;
import bio.terra.cda.app.builders.SelectBuilder;
import bio.terra.cda.app.builders.UnnestBuilder;
import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.TableSchema;
import java.io.IOException;

public class QueryHelper {
  private QueryHelper() {}

  public static QueryContext getNewQueryContext(
      String table, String fileTable, String entity, String project, boolean includeSelect)
      throws IOException {
    var schemas = new Schemas.SchemaBuilder(table, fileTable).build();
    var entitySchema = TableSchema.getDefinitionByName(schemas.getSchema(), entity);
    DataSetInfo dataSetInfo = new DataSetInfo.DataSetInfoBuilder().build();

    return new QueryContext(table, project)
        .setIncludeSelect(includeSelect)
        .setQueryFieldBuilder(
            new QueryFieldBuilder(
                schemas.getSchemaMap(), schemas.getFileSchemaMap(), dataSetInfo, table, fileTable, false))
        .setUnnestBuilder(new UnnestBuilder(table, fileTable, dataSetInfo, entitySchema.getParts(), project))
        .setPartitionBuilder(new PartitionBuilder(fileTable, dataSetInfo))
        .setSelectBuilder(new SelectBuilder(table, fileTable, dataSetInfo))
        .setParameterBuilder(
            new ParameterBuilder(schemas.getSchemaMap(), schemas.getFileSchemaMap(), dataSetInfo));
  }
}
