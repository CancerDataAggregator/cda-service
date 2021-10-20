package bio.terra.cda.app.controller;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.cda.app.service.QueryService;
import bio.terra.cda.generated.model.Query;
import bio.terra.cda.generated.model.QueryCreatedData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Disabled;
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
    var expected = "SELECT v0.* FROM TABLE.v0 AS v0 WHERE v0.test";

    var post =
        post("/api/v1/boolean-query/v0?dryRun={dryRun}&table=test", dryRun)
            .content(objectMapper.writeValueAsString(query))
            .contentType(MediaType.APPLICATION_JSON);
    var result = mvc.perform(post).andExpect(status().isOk()).andReturn();
    var response =
        objectMapper.readValue(result.getResponse().getContentAsString(), QueryCreatedData.class);
    System.out.println(response.getQuerySql());
    assertThat(response.getQuerySql(), equalTo(expected));
  }

  @Disabled
  void booleanQueryDryRun() throws Exception {
    callQueryApi(true);
    verify(queryService, never()).startQuery(anyString());

    reset(queryService);
    callQueryApi(false);
    verify(queryService, only()).startQuery(anyString());
  }

  @Test
  public void uniqueValuesTest() throws Exception {
    String version = "v3";
    String system = "GDC";
    String body = "sex";
    var expected =
        "SELECT DISTINCT sex FROM TABLE.v3, UNNEST(ResearchSubject) AS _ResearchSubject, UNNEST(_ResearchSubject.identifier) AS _identifier WHERE _identifier.system = 'GDC'";
    var result =
        mvc.perform(
                post("/api/v1/unique-values/{version}", version)
                    .param("system", system)
                    .contentType(MediaType.valueOf("text/plain"))
                    .content(body)
                    .accept(MediaType.APPLICATION_JSON))
            .andReturn();
    var response =
        objectMapper.readValue(result.getResponse().getContentAsString(), QueryCreatedData.class);
    assertThat(response.getQuerySql(), equalTo(expected));
  }
  
}
