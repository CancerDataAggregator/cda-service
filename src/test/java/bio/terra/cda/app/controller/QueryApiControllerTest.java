package bio.terra.cda.app.controller;

import bio.terra.cda.app.service.QueryService;
import bio.terra.cda.generated.model.InlineResponse200;
import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Tag("unit")
@SpringBootTest
@AutoConfigureMockMvc
class QueryApiControllerTest {

  @Autowired
  private MockMvc mvc;

  @Autowired
  private ObjectMapper objectMapper;

  @MockBean
  private QueryService queryService;

  private void callQueryApi(boolean dryRun) throws Exception {
    var query = new Query().nodeType(Query.NodeTypeEnum.COLUMN).value("test");
    var expected = "SELECT * FROM gdc-bq-sample.cda_mvp.v0 WHERE test";

    var post =
            post("/api/v1/boolean-query/v0?dryRun={dryRun}", dryRun)
            .content(objectMapper.writeValueAsString(query))
            .contentType(MediaType.APPLICATION_JSON);
    var result = mvc.perform(post).andExpect(status().isOk()).andReturn();
    var response = objectMapper.readValue(result.getResponse().getContentAsString(), InlineResponse200.class);
    assertThat(response.getQuerySql(), equalTo(expected));
  }

  @Test
  void booleanQueryDryRun() throws Exception {
    callQueryApi(true);
    verify(queryService, never()).runQuery(anyString());

    reset(queryService);
    callQueryApi(false);
    verify(queryService, only()).runQuery(anyString());
  }
}
