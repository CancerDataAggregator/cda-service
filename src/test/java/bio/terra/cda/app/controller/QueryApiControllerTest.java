package bio.terra.cda.app.controller;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.cda.app.service.QueryService;
import bio.terra.cda.generated.model.PagedResponseData;
import bio.terra.cda.generated.model.Query;
import bio.terra.cda.generated.model.QueryResponseData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.internal.matchers.Any;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Collections;
import java.util.List;

@Tag("unit")
@SpringBootTest()
@AutoConfigureMockMvc
class QueryApiControllerTest {

  @Autowired private MockMvc mvc;

  @Autowired private ObjectMapper objectMapper;

  @MockBean private QueryService queryService;

//  private void callQueryApi(boolean dryRun) throws Exception {
//    var query = new Query().nodeType(Query.NodeTypeEnum.COLUMN).value("test");
//    var expected = "SELECT v0.* FROM TABLE.v0 AS v0 WHERE v0.test";
//
//    var post =
//        post("/api/v1/boolean-query/v0?dryRun={dryRun}&table=test", dryRun)
//            .content(objectMapper.writeValueAsString(query))
//            .contentType(MediaType.APPLICATION_JSON);
//    var result = mvc.perform(post).andExpect(status().isOk()).andReturn();
//    var response =
//        objectMapper.readValue(result.getResponse().getContentAsString(), QueryResponseData.class);
//    System.out.println(response.getQuerySql());
//    assertThat(response.getQuerySql(), equalTo(expected));
//  }

  // TODO add test for unique terms with count
  
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
        "SELECT DISTINCT sex FROM subject LEFT JOIN subject_identifier AS subject_identifier ON subject.id = subject_identifier.subject_id WHERE system = 'GDC' ORDER BY sex";
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
