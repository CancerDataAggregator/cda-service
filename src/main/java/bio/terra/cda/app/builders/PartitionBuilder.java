package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.Partition;
import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.models.TableRelationship;
import java.util.Arrays;
import java.util.Objects;
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
    if (Objects.isNull(path) || path.length == 0) {
      return Stream.empty();
    }

    return Stream.concat(
        Stream.of(
            new Partition(
                path[0].getFromTableInfo().getTableName(),
                path[0].getFromTableInfo().getPartitionKeyAlias(this.dataSetInfo))),
        Arrays.stream(path)
            .map(
                tableRelationship ->
                    new Partition(
                        tableRelationship.getField(),
                        tableRelationship
                            .getDestinationTableInfo()
                            .getPartitionKeyAlias(this.dataSetInfo))));
  }

  public Partition fromQueryField(QueryField queryField) {
    TableInfo tableInfo = dataSetInfo.getTableInfoFromField(queryField.getPath());

    return new Partition(queryField.getPath(), tableInfo.getPartitionKeyAlias(this.dataSetInfo));
  }
}
