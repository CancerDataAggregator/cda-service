package bio.terra.cda.app.generators;

import bio.terra.cda.app.operators.BasicOperator;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import com.google.cloud.Tuple;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqlGenerator {
  final String qualifiedTable;
  final Query rootQuery;
  final String version;
  final String table;
  final String fileTable;
  final String project;
  final Map<String, TableSchema.SchemaDefinition> tableSchemaMap;
  final Map<String, TableSchema.SchemaDefinition> fileTableSchemaMap;
  final List<TableSchema.SchemaDefinition> tableSchema;
  final List<TableSchema.SchemaDefinition> fileTableSchema;
  final Tuple<String, TableSchema.SchemaDefinition> entitySchema;
  final List<String> filteredFields;
  final Boolean modularEntity;

  public SqlGenerator(String qualifiedTable, Query rootQuery, String version) throws IOException {
    this.qualifiedTable = qualifiedTable;
    this.rootQuery = rootQuery;
    this.version = version;
    int dotPos = qualifiedTable.lastIndexOf('.');
    this.project = dotPos == -1 ? "" : qualifiedTable.substring(0, dotPos);
    this.table = dotPos == -1 ? qualifiedTable : qualifiedTable.substring(dotPos + 1);
    this.fileTable =
        dotPos == -1
            ? qualifiedTable.replace("Subjects", "Files")
            : qualifiedTable.substring(dotPos + 1).replace("Subjects", "Files");
    this.tableSchema = TableSchema.getSchema(version);
    this.fileTableSchema = TableSchema.getSchema(fileTable);
    this.tableSchemaMap = TableSchema.buildSchemaMap(this.tableSchema);
    this.fileTableSchemaMap = TableSchema.buildSchemaMap(this.fileTableSchema);

    QueryGenerator queryGenerator = this.getClass().getAnnotation(QueryGenerator.class);
    this.modularEntity = queryGenerator != null;
    this.entitySchema =
        queryGenerator != null
            ? TableSchema.getDefinitionByName(tableSchema, queryGenerator.Entity())
            : null;

    this.filteredFields =
        queryGenerator != null ? Arrays.asList(queryGenerator.ExcludedFields()) : List.of();
  }

  public String generate() throws IllegalArgumentException {
    return sql(qualifiedTable, rootQuery, false, false, false);
  }

  public String generateFiles(Boolean global) throws IllegalArgumentException {
    return sql(qualifiedTable, rootQuery, false, true, global);
  }

  protected String sql(String tableOrSubClause, Query query, Boolean subQuery, Boolean filesQuery, Boolean globalQuery)
      throws IllegalArgumentException {
    var resultsQuery = resultsQuery(query, tableOrSubClause, subQuery, filesQuery, globalQuery);
    var resultsAlias = "results";
    return String.format(
        "SELECT %1$s.* EXCEPT(rn) FROM (%2$s) as %1$s WHERE rn = 1", resultsAlias, resultsQuery);
  }

  protected String resultsQuery(
      Query query, String tableOrSubClause, Boolean subQuery, Boolean filesQuery, Boolean globalQuery) {
    if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
      // A SUBQUERY is built differently from other queries. The FROM clause is the
      // SQL version of
      // the right subtree, instead of using table. The left subtree is now the top
      // level query.
      return resultsQuery(
          query.getL(),
          String.format("(%s)", sql(tableOrSubClause, query.getR(), true, filesQuery, globalQuery)),
          subQuery,
          filesQuery,
          globalQuery);
    }

    String[] parts = entitySchema != null ? entitySchema.x().split("\\.") : new String[0];
    String prefix = entitySchema != null ? SqlUtil.getAlias(parts.length - 1, parts) : table;

    QueryContext ctx =
            new QueryContext(
                    tableSchemaMap, tableOrSubClause, table, project, fileTable, fileTableSchemaMap);
    ctx.setEntityPath(entitySchema != null ? entitySchema.x() : "")
            .setFilesQuery(filesQuery)
            .setIncludeSelect(!subQuery);

    Stream<String> entityUnnests =
        entitySchema != null
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
    Supplier<Stream<String>> selectFields = () -> getSelect(ctx, prefix, !this.modularEntity, globalQuery);

    return String.format(
        "SELECT ROW_NUMBER() OVER (PARTITION BY %1$s) as rn, %2$s FROM %3$s WHERE %4$s",
        getPartitionByFields(ctx, ctx.getFilesQuery() ? fileTable : prefix).collect(Collectors.joining(", ")),
        subQuery ? String.format("%s.*", table) : selectFields.get().collect(Collectors.joining(", ")),
        fromString,
        condition);
  }

  protected String baseFromClause(String tableOrSubClause) {
    return tableOrSubClause + " AS " + table;
  }

  protected Stream<String> getPartitionByFields(QueryContext ctx, String alias) {
    return Stream.concat(Stream.of(String.format("%s.id", alias)), ctx.getPartitions().stream())
        .distinct();
  }

  protected Stream<String> getSelect(QueryContext ctx, String table, Boolean skipExcludes, Boolean globalQuery) {
    if (ctx.getSelect().size() > 0) {
      return ctx.getSelect().stream();
    } else {
      return getSelectsFromEntity(ctx, ctx.getFilesQuery() ? fileTable : table, skipExcludes, globalQuery);
    }
  }

  protected Stream<String> getSelectsFromEntity(
      QueryContext ctx, String prefix, Boolean skipExcludes, Boolean globalQuery) {
    ctx.addAlias("subject_id", String.format("%s.id", table));
    return Stream.concat(
            (ctx.getFilesQuery()
              ? fileTableSchema
              : entitySchema != null
                    ? List.of(entitySchema.y().getFields())
                    : tableSchema)
            .stream()
            .filter(
                definition ->
                    !(ctx.getFilesQuery()
                            && List.of("ResearchSubject", "Subject", "Specimen")
                                .contains(definition.getName()))
                        && (skipExcludes || !filteredFields.contains(definition.getName())))
            .map(definition -> {
              ctx.addAlias(definition.getName(),
                      String.format("%s%s",
                              List.of(table, fileTable).contains(prefix)
                                      ? String.format("%s.", prefix)
                                      : String.format("%s.", SqlUtil.getAntiAlias(prefix)), definition.getName()));
              return String.format("%1$s.%2$s AS %2$s", prefix, definition.getName());
            }),
            entitySchema != null && !globalQuery
              ? Stream.concat(
                    Stream.of(String.format("%s.id AS subject_id", table)),
                    SqlUtil.getIdSelectsFromPath(ctx, entitySchema.x(), ctx.getFilesQuery()))
              : Stream.of());
  }
}
