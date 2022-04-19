package bio.terra.cda.app.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryContext {
  private final String table;
  private final String tableOrSubClause;
  private List<String> unnests;
  private List<String> orderBy;
  private List<String> select;
  private final Map<String, TableSchema.SchemaDefinition> tableSchemaMap;

  public QueryContext(
      Map<String, TableSchema.SchemaDefinition> tableSchemaMap,
      String tableOrSubClause,
      String table) {
    this.tableOrSubClause = tableOrSubClause;
    this.table = table;
    this.tableSchemaMap = tableSchemaMap;

    this.unnests = new ArrayList<>();
    this.orderBy = new ArrayList<>();
    this.select = new ArrayList<>();
  }

  public void addUnnests(Stream<String> newUnnests) {
    this.unnests.addAll(newUnnests.collect(Collectors.toList()));
  }

  public void addOrderBy(String orderBy) {
    this.orderBy.add(orderBy);
  }

  public void addSelect(String select) {
    this.select.add(select);
  }

  public Map<String, TableSchema.SchemaDefinition> getTableSchemaMap() {
    return this.tableSchemaMap;
  }

  public String getTable() {
    return this.table;
  }

  public List<String> getSelect() {
    return this.select;
  }

  public List<String> getUnnests() {
    return this.unnests;
  }

  public List<String> getOrderBy() {
    return this.orderBy;
  }

  public String getTableOrSubClause() {
    return this.tableOrSubClause;
  }
}
