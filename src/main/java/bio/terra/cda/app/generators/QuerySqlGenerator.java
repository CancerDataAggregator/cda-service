package bio.terra.cda.app.generators;

import bio.terra.cda.app.builders.JoinBuilder;
import bio.terra.cda.app.builders.QueryFieldBuilder;
import bio.terra.cda.app.models.*;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlTemplate;
import bio.terra.cda.app.util.SqlUtil;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class QuerySqlGenerator extends SqlGenerator{

  private final String body;
  private final String system;
  private final boolean count;

  private String querySqlForMaxRows;
  public QuerySqlGenerator(String body, String system, boolean count) {
    this.body = body;
    this.system = system;
    this.count = count;
  }

  @Override
  public String getSqlString() {
    if (Strings.isNullOrEmpty(this.querySqlForMaxRows)) {
      generate();
    }
    return this.count ? this.querySql : this.querySqlForMaxRows;
  }

  public String getSqlStringForMaxRows() {
    if (Strings.isNullOrEmpty(this.querySqlForMaxRows)) {
      this.querySqlForMaxRows = generateForMaxRows();
    }
    return this.querySqlForMaxRows;
  }

  private String generateForMaxRows() {
    generate();
    return querySqlForMaxRows;
  }

  /***
   * This will generate a sql query String for uniqueValues
   * @return
   * @throws IllegalArgumentException
   */
  @Override
  protected String generate() throws IllegalArgumentException {
    DataSetInfo dataSetInfo = RdbmsSchema.getDataSetInfo();
    QueryFieldBuilder queryFieldBuilder = new QueryFieldBuilder(false);
    QueryField queryField = queryFieldBuilder.fromPath(body);

    TableInfo tableInfo = dataSetInfo.getTableInfoFromField(body);
    String tableName = tableInfo.getTableName();
    String identifierTablePrefix = tableName; // just a default - will validate below
    String whereClause = "";
    String fk = "";

    if (system != null && system.length() > 0) {
      String systemParam = this.parameterBuilder.addParameterValue("text",system);
      String toTable = tableName + "_identifier";

      if (dataSetInfo.getTableInfo(toTable) == null) {
        // this block only executes when the table is the somatic_mutations table which doesn't have an identifier table
        // so we use the subject_identifier table instead
        toTable = "subject_identifier";
        identifierTablePrefix = "subject";
        fk = "cda_subject_id";
      } else {
        final String finalToTable = toTable;
        fk = tableInfo.getForeignKeys().stream().filter(foreignKey -> foreignKey.getDestinationTableName().equals(finalToTable)).map(ForeignKey::getFromField).findFirst().get();
      }
      whereClause = String.format(" WHERE %s IN (SELECT DISTINCT(%s_alias) FROM %s WHERE system = %s)", fk, identifierTablePrefix, toTable, systemParam);
    }

    querySql =
          "SELECT"
              + " "
              + queryField.getName()
              + ","
              + "COUNT(*"
              + ") AS Count "
              + "FROM "
              + tableName
              + whereClause
              + " GROUP BY "
              + queryField.getName()
              + " "
              + "ORDER BY "
              + queryField.getName();

    querySqlForMaxRows =
          "SELECT DISTINCT "
              + queryField.getName()
              + " FROM "
              + tableName
              + whereClause
              + " ORDER BY "
              + queryField.getName();
    return count ? querySql : querySqlForMaxRows;
  }
}
