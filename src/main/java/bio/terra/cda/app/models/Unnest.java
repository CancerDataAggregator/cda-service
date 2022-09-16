package bio.terra.cda.app.models;

import bio.terra.cda.app.util.SqlTemplate;
import bio.terra.cda.app.util.SqlUtil;

public class Unnest {
  private final SqlUtil.JoinType joinType;
  private final String path;
  private final String alias;
  private final boolean isJoin;
  private final String joinPath;

  public Unnest(
      SqlUtil.JoinType joinType, String path, String alias, boolean isJoin, String joinPath) {
    this.joinType = joinType;
    this.path = path;
    this.alias = alias;
    this.isJoin = isJoin;
    this.joinPath = joinPath;
  }

  public Unnest(SqlUtil.JoinType joinType, String path, String alias) {
    this.joinType = joinType;
    this.path = path;
    this.alias = alias;
    this.isJoin = false;
    this.joinPath = null;
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
        : SqlTemplate.unnest(joinType.value, path, alias);
  }

  public boolean getIsJoin() {
    return isJoin;
  }
}
