package bio.terra.cda.app.util;

import bio.terra.cda.generated.model.Query;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Class to translate the endpoint Query object to a Big Query query string. */
public class QueryTranslator {

  /**
   * Create a SQL query string given a table (or subquery) and a Query object.
   *
   * @param table the table to use as the first element of the FROM clause
   * @param query the Query object
   * @return a SQL query string
   */
  public static String sql(String table, Query query) {
    return new SqlGenerator(table, query).generate();
  }

  // A convenience class to avoid having to pass 'table' around to all the methods.
  private static class SqlGenerator {
    final String qualifiedTable;
    final Query rootQuery;
    final String table;
    private Boolean hasGroupBy = false;
    private String selectInValue;
    private String tableValueList;
    private Map<String,String> paramValues = new HashMap<>();

    private SqlGenerator(String qualifiedTable, Query rootQuery) {
      this.qualifiedTable = qualifiedTable;
      this.rootQuery = rootQuery;
      int dotPos = qualifiedTable.lastIndexOf('.');
      this.table = dotPos == -1 ? qualifiedTable : qualifiedTable.substring(dotPos + 1);
    }

    private String generate() {
      return sql(qualifiedTable, rootQuery);
    }

    private String sql(String tableOrSubClause, Query query) {
      if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
        // A SUBQUERY is built differently from other queries. The FROM clause is the SQL version of
        // the right subtree, instead of using table. The left subtree is now the top level query.
        return sql(String.format("(%s)", sql(tableOrSubClause, query.getR())), query.getL());
      }
      Query tmpQuery = query;
    if(query.getNodeType() == Query.NodeTypeEnum.SELECT){
       tableValueList = query.getL().getValue();
       tmpQuery = query.getR();


    }


      var fromClause =
          Stream.concat(
                  Stream.of(tableOrSubClause + " AS " + table), getUnnestColumns(query).distinct())
              .collect(Collectors.joining(", "));

      var condition = queryString(tmpQuery);
      if(hasGroupBy && !(tableValueList == null)){
        selectInValue = Arrays.stream(tableValueList.split(","))
                .map(e -> {
                          String[] value = e.split("\\.");
                          if (e.equals("id")) {
                            return String.format("%s.%s", table.trim(), e.trim());
                          } else {
                            if(e.contains(".")){
                              return String.format("ANY_VALUE(%s) as %s",
                                      String.format("_%s.%s",value[value.length -2].trim(),value[value.length -1].trim()),
                                      value[value.length -1].trim()
                                      );
                            }
                            return String.format("ANY_VALUE(%s) as %s",e.trim(),e.trim());
                          }
                        }

                ).collect(Collectors.joining(", "));
        condition += String.format("\n GROUP BY %s.id",table);
        return String.format("SELECT %s FROM %s WHERE %s", selectInValue, fromClause, condition);
      }
      return String.format("SELECT %s.* FROM %s WHERE %s", table, fromClause, condition);
    }


    private String sql(String tableOrSubClause, Query query, String selectClause ) {
      if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
        // A SUBQUERY is built differently from other queries. The FROM clause is the SQL version of
        // the right subtree, instead of using table. The left subtree is now the top level query.
        return sql(String.format("(%s)", sql(tableOrSubClause, query.getR())), query.getL());
      }




      var fromClause =
              Stream.concat(
                      Stream.of(tableOrSubClause + " AS " + table), getUnnestColumns(query).distinct())
                      .collect(Collectors.joining(", "));

      var condition = queryString(query);

        condition += String.format("\n GROUP BY %s \n HAVING COUNT(*) >= 1",selectClause );
      return  String.format("SELECT %s FROM %s WHERE %s", selectClause, fromClause, condition);
    }

    private Stream<String> getUnnestColumns(Query query) {
      switch (query.getNodeType()) {
        case SELECTVALUES:

          return Arrays.stream(query.getValue().split(",")).flatMap(this::partsToUnnest);
        case IN:
        case QUOTED:
        case UNQUOTED:
          return Stream.empty();
        case COLUMN:
          return partsToUnnest(query.getValue().trim());
        case NOT:
          return getUnnestColumns(query.getL());
        default:
          return Stream.concat(getUnnestColumns(query.getL()), getUnnestColumns(query.getR()));
      }
    }
    private Stream<String> partsToUnnest(String value){
      // filter out table in UNNEST
      List<String> list = new ArrayList<>();
      for (String s : value.split("\\.")) {
        if (!s.equals(table)) {
          list.add(s.trim());
        }
      }
      var parts = list.toArray();
      hasGroupBy = parts.length > 1;
      return IntStream.range(0, parts.length - 1)
              .mapToObj(
                      i ->
                              i == 0
                                      ? String.format("UNNEST(%1$s) AS _%1$s", parts[i])
                                      : String.format("UNNEST(_%1$s.%2$s) AS _%2$s", parts[i - 1], parts[i])
              );
    }
    private String queryString(Query query) {
      switch (query.getNodeType()) {
        case SELECTVALUES:
          return "";
        case QUOTED:
          return String.format("'%s'", query.getValue());
        case UNQUOTED:
          return String.format("%s", query.getValue());
        case IN:
          hasGroupBy = Boolean.TRUE;

            return String.format("%s IN (%s)",query.getL().getValue(),sql(qualifiedTable,query.getR(),query.getL().getValue()));
        case COLUMN:
          var parts = query.getValue().split("\\.");
          if (parts.length > 1) {
            return String.format("_%s.%s", parts[parts.length - 2], parts[parts.length - 1]);
          }
          // Top level fields must be scoped by the table name, otherwise they could conflict with
          // unnested fields.
          return String.format("%s.%s", table, query.getValue());
        case NOT:
          return String.format("(%s %s)", query.getNodeType(), queryString(query.getL()));
        default:
          try {
            System.out.println(query.getR().getNodeType());
            if (query.getR().getNodeType().toString().equals("quoted")) {
              String paramName = String.format("%s", query.getR().getValue());
              paramValues.put(paramName, query.getR().getValue());
              System.out.println(paramValues);
            }
          }catch (Exception e){
            System.out.println(e.getMessage());
          }
          return String.format(
              "(%s %s %s)",
              queryString(query.getL()), query.getNodeType(), queryString(query.getR()));
      }
    }
  }
}
