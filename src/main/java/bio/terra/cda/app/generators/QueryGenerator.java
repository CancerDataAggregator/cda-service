package bio.terra.cda.app.generators;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface QueryGenerator {
  public String entity();

  public String[] aggregatedFields();
  public String[] aggregatedFieldsSelectString();

  public boolean hasFiles();

  public String defaultOrderBy();
}
