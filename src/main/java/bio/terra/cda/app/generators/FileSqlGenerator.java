package bio.terra.cda.app.generators;

import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlTemplate;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.generated.model.Query;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FileSqlGenerator extends SqlGenerator {
  private final List<TableInfo> tableInfoList;

  public FileSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version, true);

      tableInfoList = getTableInfosAsSortedList();
  }

  @Override
  protected String sql(String tableOrSubClause, Query query, boolean subQuery)
      throws UncheckedExecutionException, IllegalArgumentException {
    StringBuilder sb = new StringBuilder();
    AtomicReference<String> previousAlias = new AtomicReference<>("");
    List<String> tables = new ArrayList<>();
      tableInfoList.stream()
        .filter(
            Objects::nonNull) // null = subject, don't need that here as subjects are a superset of
        // researchsubject and specimen
        .forEach(
            tableInfo -> {
              var resultsQuery =
                  resultsQuery(
                      query,
                      tableOrSubClause,
                      subQuery,
                      buildQueryContext(tableInfo, true, subQuery));
              var resultsAlias =
                  String.format("%s_files", tableInfo.getTableAlias());

              var realParts = entitySchema.getParts();
              List<String> aliases =
                  IntStream.range(0, realParts.length)
                      .mapToObj(
                          i -> {
                            String realAlias = SqlUtil.getAlias(i, realParts);
                            String tmp = realAlias.substring(1).toLowerCase();
                            return String.format("%s_id", tmp);
                          })
                      .collect(Collectors.toList());
              aliases.add("subject_id");

              sb.append(
                  String.format(
                      "%1$s%2$s as (%3$s),",
                      previousAlias.get().equals("") ? "with" : "",
                      String.format(" %s", resultsAlias),
                      String.format(
                          "%s%s",
                          SqlTemplate.resultsWrapper(resultsQuery),
                          previousAlias.get().equals("")
                              ? ""
                              : String.format(
                                  " AND CONCAT(results.id, %1$s) not in (SELECT CONCAT(%2$s.id, %3$s) FROM %2$s)",
                                  aliases.stream()
                                      .map(
                                          a ->
                                              String.format(
                                                  SqlUtil.ALIAS_FIELD_FORMAT, "results", a))
                                      .collect(Collectors.joining(", ")),
                                  previousAlias,
                                  aliases.stream()
                                      .map(
                                          a ->
                                              String.format(
                                                  SqlUtil.ALIAS_FIELD_FORMAT, previousAlias, a))
                                      .collect(Collectors.joining(", "))))));
              previousAlias.set(resultsAlias);
              tables.add(resultsAlias);
            });

    sb.append(
        String.format(
            "unioned_result as (%s) ",
            tables.stream()
                .map(alias -> String.format("SELECT %1$s.* FROM %1$s", alias))
                .collect(Collectors.joining(" UNION ALL "))));

    sb.append("SELECT unioned_result.* FROM unioned_result");

    return sb.toString();
  }

  @Override
  protected Stream<String> getSelectsFromEntity(
      QueryContext ctx, String prefix, boolean skipExcludes) {

    List<String> idSelects = new ArrayList<>();
    tableInfoList.forEach(
        tableInfo -> {
          String tmp = tableInfo.getTableAlias().substring(1).toLowerCase();
          String alias = String.format("%s_id", tmp);
          var pathParts = tableInfo.getTablePath();
          var realParts = ctx.getTableInfo().getTablePath();
          String value =
              realParts.length < pathParts.length ? "''" : String.format("%s.id", ctx.getTableInfo().getTableAlias());

//          idSelects.add(
//              path.equals(EntitySchema.DEFAULT_PATH)
//                  ? String.format("%s.id as %s_id", table, EntitySchema.DEFAULT_PATH.toLowerCase())
//                  : String.format("%s AS %s", value, alias));

//          if (!path.equals(EntitySchema.DEFAULT_PATH)) {
//            ctx.addPartitions(this.partitionBuilder.fromParts(realParts, tableSchemaMap));
//          } else {
//            ctx.addPartitions(
//                Stream.of(this.partitionBuilder.of("id", String.format("%s.id", table))));
//          }
        });

    return combinedSelects(ctx, prefix, true, idSelects.stream().distinct());
  }

  private List<TableInfo> getTableInfosAsSortedList() {
    return getFileClasses()
        .map(
            clazz -> {
              var annotation = clazz.getAnnotation(QueryGenerator.class);
              return this.dataSetInfo.getTableInfo(annotation.entity());
            })
        .sorted(
            (table1, table2) -> {
              var firstSplit = table1.getTablePath();
              var secondSplit = table2.getTablePath();

              return Integer.compare(secondSplit.length, firstSplit.length);
            })
        .collect(Collectors.toList());
  }
}
