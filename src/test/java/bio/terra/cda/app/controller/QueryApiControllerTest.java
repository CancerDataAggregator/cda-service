package bio.terra.cda.app.controller;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import bio.terra.cda.app.configuration.ApplicationConfiguration;
import bio.terra.cda.app.models.RdbmsSchema;
import bio.terra.cda.app.service.QueryService;
import bio.terra.cda.generated.model.PagedResponseData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Collections;
import java.util.List;

@Tag("unit")
@ContextConfiguration(classes = QueryApiController.class)
@WebMvcTest
class QueryApiControllerTest {

  @Autowired
  private MockMvc mvc;

  @Autowired
  private ObjectMapper objectMapper;

  @MockBean
  private QueryService queryService;

  @MockBean
  private ApplicationConfiguration appConfig;

  @MockBean
  private RdbmsSchema rdbmsSchema;

  @Test
  void uniqueValuesTest() throws Exception {
    String system = "GDC";
    String body = "sex";
    Boolean count = Boolean.FALSE;

    // mock the startQuery to return the query that is passed to it as a response
    when(queryService.runQuery(anyString()))
        .thenAnswer(
            a -> {
              List<JsonNode> result = Collections.emptyList();
              return result;
            });

    var expected =
        "SELECT DISTINCT sex FROM subject WHERE integer_id_alias IN (SELECT DISTINCT(subject_alias) FROM subject_identifier WHERE system = 'GDC') ORDER BY sex  LIMIT 100";
    var result =
        mvc.perform(
                post("/api/v1/unique-values")
                    .param("system", system)
                    .param("count", String.valueOf(count))
                    .contentType(MediaType.valueOf("text/plain"))
                    .content(body)
                    .accept(MediaType.APPLICATION_JSON))
            .andReturn();
    var response =
        objectMapper.readValue(result.getResponse().getContentAsString(), PagedResponseData.class);

    assertThat(response.getQuerySql(), equalTo(expected));
  }
}
