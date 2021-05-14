package bio.terra.cda.app.controller;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.cda.app.service.QueryService;
import bio.terra.cda.generated.model.Query;
import bio.terra.cda.generated.model.QueryCreatedData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@Tag("unit")
@SpringBootTest(properties = "cda.bqTable=TABLE")
@AutoConfigureMockMvc
class QueryApiControllerTest {

  @Autowired private MockMvc mvc;

  @Autowired private ObjectMapper objectMapper;

  @MockBean private QueryService queryService;

  private void callQueryApi(boolean dryRun) throws Exception {
    var query = new Query().nodeType(Query.NodeTypeEnum.COLUMN).value("test");
    var expected = "SELECT p.* FROM TABLE.v0 AS p WHERE v0.test";

    var post =
        post("/api/v1/boolean-query/v0?dryRun={dryRun}", dryRun)
            .content(objectMapper.writeValueAsString(query))
            .contentType(MediaType.APPLICATION_JSON);
    var result = mvc.perform(post).andExpect(status().isOk()).andReturn();
    var response =
        objectMapper.readValue(result.getResponse().getContentAsString(), QueryCreatedData.class);
    assertThat(response.getQuerySql(), equalTo(expected));
  }

  @Test
  void booleanQueryDryRun() throws Exception {
    callQueryApi(true);
    verify(queryService, never()).startQuery(anyString(), any());

    reset(queryService);
    callQueryApi(false);
    verify(queryService, only()).startQuery(anyString(), any());
  }
}
