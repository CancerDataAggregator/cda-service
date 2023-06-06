package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.*;
import bio.terra.cda.app.util.SqlUtil;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public class QueryFieldBuilder {

  @Autowired
  RdbmsSchema rdbmsSchema;

  private final boolean filesQuery;
  private final DataSetInfo dataSetInfo;

  public QueryFieldBuilder(boolean filesQuery) {
    this.dataSetInfo = rdbmsSchema.getDataSetInfo();
    this.filesQuery = filesQuery;
  }

  public QueryField fromPath(String path) {
    String[] modSplit = path.split(" ");
    String modPath = modSplit[0];
    String[] parts = SqlUtil.getParts(modPath);
    ColumnDefinition columnDefinition =
        dataSetInfo.getColumnDefinitionByFieldName(modPath);
    TableInfo tableInfo = dataSetInfo.getTableInfoFromField(modPath);

    if (Objects.isNull(columnDefinition)) {
      throw new IllegalArgumentException(String.format("Column %s does not exist", path));
    }

    String alias = path;
    String modifier = "ASC";
    String columnText = getColumnText(columnDefinition, tableInfo.getTableAlias(this.dataSetInfo));

    var nonEmpties = Arrays.stream(modSplit).filter(e -> !e.isEmpty()).collect(Collectors.toList());
    if (nonEmpties.size() >= 2) {
      modifier = nonEmpties.get(1);
      alias = nonEmpties.get(0);
    }

    if (nonEmpties.size() >= 3) {
      alias = nonEmpties.get(2);
    }

    return new QueryField(
        columnDefinition.getName(),
        modPath,
        alias,
        tableInfo.getTableName(),
        modifier,
        columnDefinition,
        filesQuery,
        tableInfo.getTableName().equals(RdbmsSchema.FILE_TABLE));
  }

  protected String getColumnText(ColumnDefinition columnDefinition, String tableAlias) {
    return columnDefinition.getDescription();
  }
}
