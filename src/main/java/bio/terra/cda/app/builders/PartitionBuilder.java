package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.Partition;
import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.models.TableRelationship;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class PartitionBuilder {
  private final DataSetInfo dataSetInfo;

  public PartitionBuilder(DataSetInfo dataSetInfo) {
    this.dataSetInfo = dataSetInfo;
  }

  public Partition of(String path, String text) {
    return new Partition(path, text);
  }

  public Stream<Partition> fromRelationshipPath(TableRelationship[] path) {
    return Arrays.stream(path)
            .map(tableRelationship -> new Partition(
                    tableRelationship.getField(),
                    tableRelationship.getFromTableInfo().getPartitionKeyAlias()));
  }

  public Partition fromQueryField(QueryField queryField) {
    TableInfo tableInfo = dataSetInfo.getTableInfoFromField(queryField.getPath());

    return new Partition(queryField.getPath(), tableInfo.getPartitionKeyAlias());
  }
}
