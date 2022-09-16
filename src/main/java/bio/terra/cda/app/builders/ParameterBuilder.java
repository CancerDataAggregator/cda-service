package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.QueryField;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ParameterBuilder {
  private final Map<String, QueryParameterValue> parameterValueMap;

  public ParameterBuilder() {
    this.parameterValueMap = new HashMap<>();
  }

  public Map<String, QueryParameterValue> getParameterValueMap() {
    return parameterValueMap;
  }

  public String addParameterValue(QueryField queryField, Object value) {
    String parameterName = String.format("%s_1", queryField.getAlias());

    while (this.parameterValueMap.containsKey(parameterName)) {
      String number = parameterName.substring(parameterName.length() - 1);
      int index = Integer.parseInt(number);
      parameterName =
          String.format("%s%s", parameterName.substring(0, parameterName.length() - 1), ++index);
    }

    QueryParameterValue queryParameterValue;

    StandardSQLTypeName fieldType =
        LegacySQLTypeName.valueOf(queryField.getType()).getStandardType();

    if (value.getClass().isArray()) {
      queryParameterValue =
          QueryParameterValue.newBuilder()
              .setArrayValues(
                  Arrays.stream((String[]) value)
                      .map(
                          val ->
                              QueryParameterValue.newBuilder()
                                  .setType(fieldType)
                                  .setValue(val)
                                  .build())
                      .collect(Collectors.toList()))
              .setType(StandardSQLTypeName.ARRAY)
              .setArrayType(fieldType)
              .build();
    } else {
      queryParameterValue =
          QueryParameterValue.newBuilder().setType(fieldType).setValue((String) value).build();
    }

    this.parameterValueMap.put(parameterName, queryParameterValue);

    return String.format("@%s", parameterName);
  }
}
