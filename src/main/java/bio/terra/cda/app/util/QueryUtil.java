package bio.terra.cda.app.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum QueryUtil {
  _UNUSED;

  interface Node {
  }
  static class Column implements Node {
    public final String name;
    Column(String name) {
      this.name = name;
    }
    public String toString() {
      if (parent() != null) {
        return "_" + name;
      }
      return name;
    }
    public String parent() {
      var parts = name.split("\\.");
      if (parts.length > 1) {
        return parts[0];
      }
      return null;
    }
  }
  static class Value implements Node {
    public final Object value;
    Value(Object value) {
      this.value = value;
    }

    public String toString() {
      if (value instanceof String) {
        return String.format("'%s'", value);
      } else if (value == null) {
        return "NULL";
      }
      return value.toString();
    }
  }
  static class Condition implements Node {
    public final Node left;
    public final String operator;
    public final Node right;

    public Condition(Node column, String operator, Node value) {
      this.left = column;
      this.operator = operator;
      this.right = value;
    }

    public Condition(String column, String operator, int value) {
      this(new Column(column), operator, new Value(value));
    }

    public Condition(String column, String operator, String value) {
      this(new Column(column), operator, new Value(value));
    }

    Condition And(Condition right) {
      return new Condition(this, "AND", right);
    }

    Condition Or(Condition right) {
      return new Condition(this, "OR", right);
    }

    private Stream<String> unnestRecurse(Node node) {
      if (node instanceof Column) {
        return Stream.of(((Column) node).parent());
      } else if (node instanceof Condition) {
        return ((Condition) node).columnsToUnnest();
      }
      return Stream.empty();
    }

    Stream<String> columnsToUnnest() {
      return Stream.concat(unnestRecurse(left), unnestRecurse(right));
    }

    @Override
    public String toString() {
      return String.format("(%s %s %s)", left, operator, right);
    }
  }

  static class Dataset {
    public final String table;
    public final Condition condition;

    public Dataset(String table, Condition condition) {
      this.table = table;
      this.condition = condition;
    }

    String sql() {
      var fromClause =
              Stream.concat(Stream.of(table),
                      condition.columnsToUnnest()
                              .filter(Objects::nonNull)
                              .distinct()
                              .map(s -> String.format("UNNEST(%1$s) AS _%1$s", s)))
                      .collect(Collectors.joining(", "));

      return String.format("SELECT * FROM %s WHERE %s", fromClause, condition);
    }
  }
}
