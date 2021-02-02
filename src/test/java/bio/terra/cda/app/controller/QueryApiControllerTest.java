package bio.terra.cda.app.controller;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import bio.terra.cda.app.service.QueryService;
import bio.terra.cda.generated.model.Query;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest
class QueryApiControllerTest {

  @MockBean QueryService queryService;

  @Test
  void booleanQuery() {
    var controller = new QueryApiController(null, queryService);
    var query = new Query().nodeType(Query.NodeTypeEnum.COLUMN).value("test");
    var result = controller.booleanQuery("version", query, 0, 0, true);
    verify(queryService, never()).runQuery(anyString());

    reset(queryService);
    var result2 = controller.booleanQuery("version", query, 0, 0, false);
    verify(queryService, only()).runQuery(anyString());
  }
}
