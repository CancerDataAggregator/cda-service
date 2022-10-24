package bio.terra.cda.app.models;

import bio.terra.cda.app.util.SqlTemplate;
import bio.terra.cda.app.util.SqlUtil;
import java.util.Objects;

public class Unnest {
  private final SqlUtil.JoinType joinType;
  private final String path;
  private final String alias;
  private final boolean isJoin;
  private final String joinPath;
  private final TableInfo tableInfo;

  public Unnest(
      SqlUtil.JoinType joinType,
      String path,
      String alias,
      boolean isJoin,
      String joinPath,
      TableInfo tableInfo) {
    this.joinType = joinType;
    this.path = path;
    this.alias = alias;
    this.isJoin = isJoin;
    this.joinPath = joinPath;
    this.tableInfo = tableInfo;
  }

  public Unnest(SqlUtil.JoinType joinType, String path, String alias, TableInfo tableInfo) {
    this.joinType = joinType;
    this.path = path;
    this.alias = alias;
    this.isJoin = false;
    this.joinPath = null;
    this.tableInfo = tableInfo;
  }

  public SqlUtil.JoinType getJoinType() {
    return joinType;
  }

  public String getPath() {
    return path;
  }

  public String getAlias() {
    return alias;
  }

  @Override
  public String toString() {
    return isJoin
        ? SqlTemplate.join(joinType.value, path, alias, joinPath)
        : SqlTemplate.unnest(
            joinType.value, path, alias, Objects.nonNull(joinPath) ? joinPath : "");
  }
}
