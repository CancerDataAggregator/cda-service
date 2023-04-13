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
  private int index;

  public ParameterBuilder() {
    this.parameterValueMap = new HashMap<>();
    this.index = 0;
  }

  public Map<String, QueryParameterValue> getParameterValueMap() {
    return parameterValueMap;
  }

  public String addParameterValue(String type, Object value) {
    String parameterName = String.format("parameter_%s", ++index);
    QueryParameterValue queryParameterValue;

    StandardSQLTypeName fieldType = LegacySQLTypeName.valueOf(type).getStandardType();

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

  public String addParameterValue(QueryField queryField, Object value) {
    return this.addParameterValue(queryField.getType(), value);
  }
}
