package bio.terra.cda.app.util;

public class Unnest {
    private final SqlUtil.JoinType joinType;
    private final String path;
    private final String alias;
    private Boolean isJoin;
    private String firstJoinPath;
    private String secondJoinPath;

    public Unnest(SqlUtil.JoinType joinType, String path, String alias) {
        this.joinType = joinType;
        this.path = path;
        this.alias = alias;
        this.isJoin = false;
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
            ? SqlTemplate.join(joinType.value, path, alias, firstJoinPath, secondJoinPath)
            : SqlTemplate.unnest(joinType.value, path, alias);
    }

    public Boolean getIsJoin() {
        return isJoin;
    }

    public Unnest setIsJoin(Boolean isJoin) {
        this.isJoin = isJoin;
        return this;
    }

    public Unnest setFirstJoinPath(String joinPath) {
        this.firstJoinPath = joinPath;
        return this;
    }

    public Unnest setSecondJoinPath(String joinPath) {
        this.secondJoinPath = joinPath;
        return this;
    }
}
