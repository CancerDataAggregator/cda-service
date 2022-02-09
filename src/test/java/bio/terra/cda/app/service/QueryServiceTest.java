package bio.terra.cda.app.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.stereotype.Component;

@Component
class QueryServiceTest {

  private final QueryService queryService = new QueryService(new ObjectMapper());

  private static Stream<Arguments> valueToJson() {
    return Stream.of(
        arguments(
            FieldValue.of(
                FieldValue.Attribute.RECORD,
                FieldValueList.of(
                    List.of(
                        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "value1"),
                        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123456"),
                        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "false")))),
            Field.of(
                "root",
                StandardSQLTypeName.STRUCT,
                Field.of("text", StandardSQLTypeName.STRING),
                Field.of("number", StandardSQLTypeName.NUMERIC),
                Field.of("boolean", StandardSQLTypeName.BOOL)),
            "{\"text\":\"value1\",\"number\":123456,\"boolean\":false}"),
        arguments(
            FieldValue.of(
                FieldValue.Attribute.REPEATED,
                FieldValueList.of(
                    List.of(
                        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123"),
                        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "456")))),
            Field.of("root", StandardSQLTypeName.NUMERIC),
            "[123,456]"));
  }

  @Disabled
  @ParameterizedTest
  @MethodSource("valueToJson")
  void testValueToJson(FieldValue value, Field field, String expected) {
    final JsonNode jsonNode = queryService.valueToJson(value, field);
    assertThat(jsonNode.toString(), equalTo(expected));
  }
}
