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
    this.body = body.toLowerCase();
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
    List<String> whereClauses = new ArrayList<>();
    JoinBuilder jb = new JoinBuilder();
    List<Join> pathToSystem = Collections.emptyList();

    if (system != null && system.length() > 0) {
      String systemParam = this.parameterBuilder.addParameterValue("text",system);
      String toTable = tableName + "_identifier";
      if (dataSetInfo.getTableInfo(toTable) == null) {
        toTable = "subject_identifier";
      }
      pathToSystem = jb.getPath(tableName, toTable, "system", SqlUtil.JoinType.LEFT);


      QueryField systemField =
          queryFieldBuilder.fromPath( toTable + "_system");
      whereClauses.add(systemField.getName() + " = " + systemParam);
    }

    whereClauses.addAll(pathToSystem.stream().map(join -> SqlTemplate.joinCondition(join)).distinct().collect(Collectors.toList()));

    String whereStr = "";
    if (!whereClauses.isEmpty()) {
      whereStr = " WHERE " + String.join(" AND ", whereClauses);
    }

    String joins = pathToSystem.stream().map(join -> SqlTemplate.join(join)).distinct().collect(Collectors.joining(", "));
    if (!joins.isEmpty()) {
      joins = ", " + joins;
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
              + joins
              + whereStr
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
              + joins
              + whereStr
              + " ORDER BY "
              + queryField.getName();
    return count ? querySql : querySqlForMaxRows;
  }
}
