package bio.terra.cda.app.models;

import org.apache.logging.log4j.util.Strings;

public class ManualView extends View {

  private final String sql;

  public ManualView(String sql) {
    super(
        Strings.EMPTY,
        ViewType.INLINE,
        Strings.EMPTY,
        Strings.EMPTY,
        Strings.EMPTY,
        Strings.EMPTY,
        false);
    this.sql = sql;
  }

  public String toString() {
    return sql;
  }
}
