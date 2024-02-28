package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.QueryField;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.util.StringUtils;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ParameterBuilder {
  private static final Logger logger = LoggerFactory.getLogger(ParameterBuilder.class);
  private final MapSqlParameterSource parameterValueMap;
  private int index;

  public ParameterBuilder() {
    this.parameterValueMap = new MapSqlParameterSource();
    this.index = 0;
  }

  public MapSqlParameterSource getParameterValueMap() {
    return parameterValueMap;
  }

  public String addParameterValue(String type, Object value) {
    String parameterName = String.format("parameter_%s", ++index);

    if (value.getClass().isArray()) {
      this.parameterValueMap.addValue(parameterName, value, Types.ARRAY);
    } else if (type.equals("text")) {
      this.parameterValueMap.addValue(parameterName, value);
    } else if (type.equals("integer")) {
      this.parameterValueMap.addValue(parameterName, value, Types.INTEGER);
    } else if (type.equals("float")) {
      this.parameterValueMap.addValue(parameterName, value, Types.FLOAT);
    } else if (type.equals("boolean")) {
      this.parameterValueMap.addValue(parameterName, value, Types.BOOLEAN);
    } else {
      logger.error("Unknown type: {}. Trying to add anyway", type);
      this.parameterValueMap.addValue(parameterName, value);
    }
    return String.format(":%s", parameterName);
  }

  public String substituteForReadableString(String sqlStr) {
    String result = sqlStr;
    for (String key : getParameterValueMap().getParameterNames()) {
      String keyformat = String.format(":%s", key);
      Object value = parameterValueMap.getValue(key);
      int type = parameterValueMap.getSqlType(key);
      if (type == Types.INTEGER || type == Types.FLOAT || type == Types.BOOLEAN) {
        result = result.replace(keyformat, value.toString());
      } else if (type == Types.ARRAY) {
        List<String> valueList = Arrays.stream((Object[])value).map(x ->
            StringUtils.quoteIfString(x).toString()).collect(Collectors.toList());
        result = result.replace(keyformat, String.format("(%s)", Strings.join(valueList, ',')));
      } else
      {
        result = result.replace(keyformat, StringUtils.quoteIfString(value).toString());
      }
    }
    return result;
  }

  public String addParameterValue(QueryField queryField, Object value) {
    return this.addParameterValue(queryField.getType(), value);
  }
}
