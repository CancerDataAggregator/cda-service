package bio.terra.cda.app.helpers;

import bio.terra.cda.app.builders.OrderByBuilder;
import bio.terra.cda.app.builders.ParameterBuilder;
import bio.terra.cda.app.builders.QueryFieldBuilder;
import bio.terra.cda.app.builders.SelectBuilder;
import bio.terra.cda.app.builders.ViewBuilder;
import bio.terra.cda.app.builders.ViewListBuilder;
import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.View;
import bio.terra.cda.app.util.QueryContext;
import java.io.IOException;

public class QueryHelper {
  private QueryHelper() {}

//  public static QueryContext getNewQueryContext(
//      String table, String fileTable, String entity, String project, boolean includeSelect)
//      throws IOException {
//    var schemas = new Schemas.SchemaBuilder(table, fileTable).build();
//    DataSetInfo dataSetInfo =
//        new DataSetInfo.DataSetInfoBuilder()
//            .addTableSchema("all_Subjects_v3_0_final", schemas.getSchema())
//            .build();
//    QueryFieldBuilder queryFieldBuilder = new QueryFieldBuilder(dataSetInfo, false);
//    ViewListBuilder<View, ViewBuilder> viewListBuilder =
//        new ViewListBuilder<>(ViewBuilder.class, dataSetInfo, project);
//
//    return new QueryContext(table, project)
//        .setIncludeSelect(includeSelect)
//        .setQueryFieldBuilder(queryFieldBuilder)
//        .setUnnestBuilder(
//            new UnnestBuilder(
//                queryFieldBuilder,
//                viewListBuilder,
//                dataSetInfo,
//                dataSetInfo.getTableInfo(entity),
//                project))
//        .setPartitionBuilder(new PartitionBuilder(dataSetInfo))
//        .setSelectBuilder(new SelectBuilder(dataSetInfo))
//        .setParameterBuilder(new ParameterBuilder())
//        .setOrderByBuilder(new OrderByBuilder())
//        .setViewListBuilder(viewListBuilder);
//  }
}
