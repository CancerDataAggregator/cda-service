package bio.terra.cda.app.generators;

import bio.terra.cda.app.operators.BasicOperator;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import com.google.cloud.Tuple;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FileSqlGenerator extends SqlGenerator {
  private final List<Tuple<String, TableSchema.SchemaDefinition>> schemaList;

  public FileSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version);

    schemaList = getEntitySchemasAsSortedList();
  }

  @Override
  protected String sql(String tableOrSubClause, Query query, Boolean subQuery, Boolean filesQuery)
          throws UncheckedExecutionException, IllegalArgumentException {
      StringBuilder sb = new StringBuilder();
      AtomicReference<String> previousAlias = new AtomicReference<>("");
      List<String> tables = new ArrayList<>();
      schemaList
              .stream()
              .filter(Objects::nonNull) // null = subject, don't need that here as subjects are a superset of researchsubject and specimen
              .forEach(entitySchema -> {
          var resultsQuery = resultsQuery(query, tableOrSubClause, subQuery, filesQuery, entitySchema);
          var resultsAlias = String.format("%s_files", entitySchema.x().replace(".", "_"));

          var realParts = entitySchema.x().split("\\.");
          List<String> aliases = IntStream.range(0, realParts.length)
                  .mapToObj(i -> {
                      String realAlias = SqlUtil.getAlias(i, realParts);
                      String tmp = realAlias.substring(1).toLowerCase();
                      return String.format("%s_id", tmp);
                  }).collect(Collectors.toList());
          aliases.add("subject_id");

          sb.append(String.format("%1$s%2$s as (%3$s),",
                  previousAlias.get().equals("") ? "with": "",
                  String.format(" %s", resultsAlias),
                  String.format("SELECT results.* EXCEPT(rn) FROM (%s) as results WHERE rn = 1%s",
                          resultsQuery,
                          previousAlias.get().equals("")
                                  ? ""
                                  : String.format(" AND CONCAT(results.id, %1$s) not in (SELECT CONCAT(%2$s.id, %3$s) FROM %2$s)",
                                  aliases.stream()
                                          .map(a -> String.format("%s.%s", "results", a))
                                          .collect(Collectors.joining(", ")),
                                  previousAlias,
                                  aliases.stream()
                                          .map(a -> String.format("%s.%s", previousAlias, a))
                                          .collect(Collectors.joining(", "))))));
          previousAlias.set(resultsAlias);
          tables.add(resultsAlias);
      });

      sb.append(String.format("unioned_result as (%s) ",
              tables.stream().map(alias -> String.format("SELECT %1$s.* FROM %1$s", alias))
                      .collect(Collectors.joining(" UNION ALL "))));

      sb.append("SELECT unioned_result.* FROM unioned_result");

      return sb.toString();
  }

  protected String resultsQuery(
          Query query, String tableOrSubClause, Boolean subQuery, Boolean filesQuery,
          Tuple<String, TableSchema.SchemaDefinition> actualSchema) {
    if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
        // A SUBQUERY is built differently from other queries. The FROM clause is the
        // SQL version of
        // the right subtree, instead of using table. The left subtree is now the top
        // level query.
        return resultsQuery(
                query.getL(),
                String.format("(%s)", sql(tableOrSubClause, query.getR(), true, filesQuery)),
                subQuery,
                filesQuery,
                actualSchema);
    }

    String[] parts = actualSchema != null ? actualSchema.x().split("\\.") : new String[0];
    String prefix = actualSchema != null ? SqlUtil.getAlias(parts.length - 1, parts) : table;

    QueryContext ctx =
            new QueryContext(
                    tableSchemaMap, tableOrSubClause, table, project, fileTable, fileTableSchemaMap);
    ctx.setEntityPath(actualSchema != null ? actualSchema.x() : "")
            .setFilesQuery(filesQuery)
            .setIncludeSelect(!subQuery);

    Stream<String> entityUnnests =
            actualSchema != null
                    ? SqlUtil.getUnnestsFromParts(ctx, table, parts, true, SqlUtil.JoinType.INNER)
                    : Stream.empty();

    String[] filesParts =
            Stream.concat(Arrays.stream(parts), Stream.of("Files")).toArray(String[]::new);
    String filesAlias = SqlUtil.getAlias(filesParts.length - 1, filesParts);

    Stream<String> filesUnnests =
            filesQuery
                    ? SqlUtil.getUnnestsFromParts(ctx, table, filesParts, true, SqlUtil.JoinType.INNER)
                    : Stream.empty();

    String condition = ((BasicOperator) query).buildQuery(ctx);
    String selectFields =
            getSelect(ctx, prefix, actualSchema).collect(Collectors.joining(", "));

    var fromClause =
            Stream.concat(
                            Stream.concat(
                                    Stream.concat(
                                            Stream.of(baseFromClause(tableOrSubClause)), ctx.getUnnests().stream()),
                                    entityUnnests),
                            filesUnnests)
                    .distinct();

    if (filesQuery) {
        fromClause =
                Stream.concat(
                        fromClause,
                        Stream.of(
                                String.format(
                                        " %1$s %2$s AS %3$s ON %3$s.id = %4$s",
                                        SqlUtil.JoinType.INNER.value,
                                        String.format("%s.%s", this.project, this.fileTable),
                                        this.fileTable,
                                        filesAlias)));
    }

    String fromString = fromClause.distinct().collect(Collectors.joining(" "));

    return String.format(
            "SELECT ROW_NUMBER() OVER (PARTITION BY %1$s) as rn, %2$s FROM %3$s WHERE %4$s",
            getPartitionByFields(ctx, ctx.getFilesQuery() ? fileTable : prefix)
                    .collect(Collectors.joining(", ")),
            subQuery ? String.format("%s.*", table) : selectFields,
            fromString,
            condition);
  }

  protected Stream<String> getSelect(
          QueryContext ctx, String table, Tuple<String, TableSchema.SchemaDefinition> actualSchema) {
      if (ctx.getSelect().size() > 0) {
          return ctx.getSelect().stream();
      } else {
          return getSelectsFromEntity(ctx, ctx.getFilesQuery() ? fileTable : table, actualSchema);
      }
  }

  protected Stream<String> getSelectsFromEntity(
          QueryContext ctx, String prefix, Tuple<String, TableSchema.SchemaDefinition> actualSchema) {
    ctx.addAlias("subject_id", String.format("%s.id", table));

    List<String> idSelects = new ArrayList<>();
    schemaList
        .forEach(
            entitySchema -> {
              String path = entitySchema != null ? entitySchema.x() : "Subject";

              var pathParts = path.split("\\.");
              var realParts = actualSchema != null
                      ? actualSchema.x().split("\\.")
                      : new String[0];
              String realAlias = SqlUtil.getAlias(pathParts.length - 1, pathParts);
              String tmp = realAlias.substring(1).toLowerCase();
              String alias = String.format("%s_id", tmp);
              String value = realParts.length < pathParts.length
                      ? "''"
                      : String.format("%s.id", realAlias);

              ctx.addAlias(alias, path);

              idSelects.add(
                  path.equals("Subject")
                      ? String.format("%s.id as subject_id", table)
                      : String.format("%s AS %s", value, alias));

              if (!path.equals("Subject")) {
                ctx.addPartitions(
                    IntStream.range(0, realParts.length)
                        .mapToObj(i -> String.format("%s.id", SqlUtil.getAlias(i, realParts))));
              } else {
                ctx.addPartitions(Stream.of(String.format("%s.id", table)));
              }
            });

    return combinedSelects(ctx, prefix, true, idSelects.stream().distinct());
  }

  private List<Tuple<String, TableSchema.SchemaDefinition>> getEntitySchemasAsSortedList() {
      return getFileClasses()
              .map(clazz -> {
                  var annotation = clazz.getAnnotation(QueryGenerator.class);
                  return TableSchema.getDefinitionByName(tableSchema, annotation.Entity());
              })
              .sorted((schema1, schema2) -> {
                  var firstSplit = schema1 != null ? schema1.x().split("\\.") : new String[0];
                  var secondSplit = schema2 != null ? schema2.x().split("\\.") : new String[0];

                  return Integer.compare(secondSplit.length, firstSplit.length);
              }).collect(Collectors.toList());
  }
}
