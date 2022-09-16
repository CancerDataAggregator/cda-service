package bio.terra.cda.app.generators;

import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.models.TableRelationship;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlTemplate;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.generated.model.Query;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
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
    tableInfoList.forEach(
        tableInfo -> {
          var resultsQuery =
              resultsQuery(
                  query, tableOrSubClause, subQuery, buildQueryContext(tableInfo, true, subQuery));
          var resultsAlias =
              String.format("%s_files", tableInfo.getAdjustedTableName().toLowerCase(Locale.ROOT));

          TableRelationship[] tablePath = tableInfo.getTablePath();
          List<String> aliases =
              Arrays.stream(tablePath)
                  .map(
                      tableRelationship ->
                          tableRelationship
                              .getFromTableInfo()
                              .getPartitionKeyFullName(this.dataSetInfo))
                  .collect(Collectors.toList());

          aliases.add(tableInfo.getPartitionKeyFullName(this.dataSetInfo));

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
                              " AND CONCAT(results.file_id, %1$s) not in (SELECT CONCAT(%2$s.file_id, %3$s) FROM %2$s)",
                              aliases.stream()
                                  .map(a -> String.format(SqlUtil.ALIAS_FIELD_FORMAT, "results", a))
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
          var pathParts = tableInfo.getTablePath();
          var realParts = ctx.getTableInfo().getTablePath();
          String value =
              realParts.length < pathParts.length ? "''" : tableInfo.getPartitionKeyAlias();

          idSelects.add(
              String.format(
                  "%s AS %s", value, tableInfo.getPartitionKeyFullName(this.dataSetInfo)));

          if (realParts.length == 0) {
            ctx.addPartitions(
                Stream.of(
                    this.partitionBuilder.of(
                        ctx.getTableInfo().getTableName(),
                        ctx.getTableInfo().getPartitionKeyAlias())));
          } else {
            ctx.addPartitions(this.partitionBuilder.fromRelationshipPath(realParts));
          }
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
