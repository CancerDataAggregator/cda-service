package bio.terra.cda.app.operators;

import bio.terra.cda.generated.model.Query;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface QueryOperator {
  public Query.NodeTypeEnum[] nodeType();
}
