package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import com.google.cloud.bigquery.Field;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public class QueryFieldBuilder {
  private final boolean filesQuery;
  private final DataSetInfo dataSetInfo;

  public QueryFieldBuilder(DataSetInfo dataSetInfo, boolean filesQuery) {
    this.dataSetInfo = dataSetInfo;
    this.filesQuery = filesQuery;
  }

  public QueryField fromPath(String path) {
    String[] modSplit = path.split(" ");
    String modPath = modSplit[0];
    String[] parts = SqlUtil.getParts(modPath);
    TableSchema.SchemaDefinition schemaDefinition =
        dataSetInfo.getSchemaDefinitionByFieldName(modPath);
    TableInfo tableInfo = dataSetInfo.getTableInfoFromField(modPath);

    if (Objects.isNull(schemaDefinition)) {
      throw new IllegalArgumentException(String.format("Column %s does not exist", path));
    }

    String alias = path.replace(".", "_");
    String columnText = getColumnText(schemaDefinition, tableInfo.getTableAlias());

    var nonEmpties = Arrays.stream(modSplit).filter(e -> !e.isEmpty()).collect(Collectors.toList());
    if (nonEmpties.size() >= 3) {
      alias = nonEmpties.get(2);
    }

    String tableAlias = DataSetInfo.KNOWN_ALIASES.get(tableInfo.getTableName());

    return new QueryField(
        schemaDefinition.getName(),
        modPath,
        parts,
        alias,
        columnText,
        tableInfo.getAdjustedTableName(),
        schemaDefinition,
        filesQuery,
        Objects.nonNull(tableAlias) && tableAlias.equals(TableSchema.FILE_PREFIX));
  }

  protected String getColumnText(TableSchema.SchemaDefinition schemaDefinition, String tableAlias) {
    String mode = schemaDefinition.getMode();

    if (mode.equals(Field.Mode.REPEATED.toString())) {
      return tableAlias;
    } else {
      return String.format(SqlUtil.ALIAS_FIELD_FORMAT, tableAlias, schemaDefinition.getName());
    }
  }
}
