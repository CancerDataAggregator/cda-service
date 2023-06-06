package bio.terra.cda.app.models;

import bio.terra.cda.app.util.SqlTemplate;
import bio.terra.cda.app.util.SqlUtil;

public class Join {
  private final ForeignKey joinKey;

  private SqlUtil.JoinType joinType;

  // TODO could wrap FK methods
  public Join(ForeignKey joinKey, SqlUtil.JoinType joinType) {
    this.joinKey = joinKey;
    this.joinType = joinType;
  }

  public SqlUtil.JoinType getJoinType() {
    return joinType;
  }

  public void setJoinType(SqlUtil.JoinType newType) {
    this.joinType = newType;
  }

  public ForeignKey getKey() {
    return joinKey;
  }
}
