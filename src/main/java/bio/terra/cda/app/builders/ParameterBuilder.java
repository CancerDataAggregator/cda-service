package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.QueryField;
import org.apache.logging.log4j.util.Strings;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.util.StringUtils;

import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ParameterBuilder {
  private final MapSqlParameterSource parameterValueMap;

  public ParameterBuilder() {
    this.parameterValueMap = new MapSqlParameterSource();
  }

  public MapSqlParameterSource getParameterValueMap() {
    return parameterValueMap;
  }

  public String addParameterValue(QueryField queryField, Object value) {
    String parameterName = String.format("%s_1", queryField.getAlias());

    while (this.parameterValueMap.hasValue(parameterName)) {
      String number = parameterName.substring(parameterName.length() - 1);
      int index = Integer.parseInt(number);
      parameterName =
          String.format("%s%s", parameterName.substring(0, parameterName.length() - 1), ++index);
    }

    if (value.getClass().isArray()) {
      this.parameterValueMap.addValue(parameterName, value, Types.ARRAY);
    } else
    if (queryField.getType().equals("text")) {
      this.parameterValueMap.addValue(parameterName, value);
    } else if (queryField.getType().equals("integer")){
      this.parameterValueMap.addValue(parameterName, value, Types.INTEGER);
    } else if (queryField.getType().equals("float")) {
      this.parameterValueMap.addValue(parameterName, value, Types.FLOAT);
    }
    return String.format(":%s", parameterName);
  }

  public String substituteForReadableString(String sqlStr) {
    String result = sqlStr;
    for (String key : getParameterValueMap().getParameterNames()) {
      String keyformat = String.format(":%s", key);
      Object value = parameterValueMap.getValue(key);
      int type = parameterValueMap.getSqlType(key);
      if (type == Types.INTEGER || type == Types.FLOAT) {
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
}
