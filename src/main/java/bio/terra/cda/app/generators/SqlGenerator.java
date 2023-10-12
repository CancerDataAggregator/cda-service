package bio.terra.cda.app.generators;

import bio.terra.cda.app.builders.ParameterBuilder;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlTemplate;
import com.google.common.base.Strings;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

public abstract class SqlGenerator {

  protected ParameterBuilder parameterBuilder = new ParameterBuilder();
  protected String querySql;

  public MapSqlParameterSource getNamedParameterMap() {
    return this.parameterBuilder.getParameterValueMap();
  }

  public String getSqlString() {
    if (Strings.isNullOrEmpty(this.querySql)) {
      this.querySql = generate();
    }
    return this.querySql;
  }

  public String getSqlStringForMaxRows() {
    return getSqlString();
  }


  public String getReadableQuerySql() {
    // TODO should we be adding the offset and limit to the readable sql query?
    String sqlStr = getSqlString();
    return this.parameterBuilder.substituteForReadableString(sqlStr);
  }


  public String getReadableQuerySql(Integer offset, Integer limit) {
    // TODO should we be adding the offset and limit to the readable sql query?

    return SqlTemplate.addPagingFields(getReadableQuerySql(), offset, limit);
  }

  protected abstract String generate() throws IllegalArgumentException;


}
