package bio.terra.cda.app.controller;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.cda.app.service.QueryService;
import bio.terra.cda.generated.model.Query;
import bio.terra.cda.generated.model.QueryCreatedData;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.QueryJobConfiguration;
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

  // TODO add test for unique terms with count

  @Test
  void uniqueValuesTest() throws Exception {
    String version = "all_Subjects_v3_0_final";
    String system = "GDC";
    String body = "sex";
    String table = "gdc-bq-sample.dev";
    Boolean count = Boolean.FALSE;

    // mock the startQuery to return the query that is passed to it as a response
    when(queryService.startQuery((QueryJobConfiguration.Builder) any(), anyBoolean()))
        .thenAnswer(
            a -> {
              var response = new QueryCreatedData();

              QueryJobConfiguration.Builder builder = a.getArgument(0);
              response.setQuerySql(builder.build().getQuery());

              return response;
            });

    var expected =
        "SELECT DISTINCT Subject.sex FROM gdc-bq-sample.dev.all_Subjects_v3_0_final AS Subject INNER JOIN UNNEST(Subject.identifier) AS _subject_identifier WHERE _subject_identifier.system = 'GDC' ORDER BY Subject.sex";
    var result =
        mvc.perform(
                post("/api/v1/unique-values/{version}", version)
                    .param("system", system)
                    .param("table", table)
                    .param("count", String.valueOf(count))
                    .contentType(MediaType.valueOf("text/plain"))
                    .content(body)
                    .accept(MediaType.APPLICATION_JSON))
            .andReturn();
    var response =
        objectMapper.readValue(result.getResponse().getContentAsString(), QueryCreatedData.class);

    assertThat(response.getQuerySql(), equalTo(expected));
  }
}
