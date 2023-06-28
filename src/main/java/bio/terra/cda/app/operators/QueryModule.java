package bio.terra.cda.app.operators;

import bio.terra.cda.generated.model.Operator;
import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class QueryModule extends SimpleModule {
  public QueryModule() {
    super();

    addDeserializer(Query.class, new QueryDeserializer());
    addDeserializer(Operator.class, new OperatorDeserializer());
  }
}
