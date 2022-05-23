package bio.terra.cda.app.generators;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface CountQueryGenerator {
  public String Entity();

  public String[] FieldsToCount();

  public String[] ExcludedFields();
}
