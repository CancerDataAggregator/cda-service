package bio.terra.cda.app.generators;

import bio.terra.cda.app.operators.BasicOperator;
import bio.terra.cda.app.util.EntitySchema;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlTemplate;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.app.util.Unnest;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

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
  final Boolean filesQuery;
  EntitySchema entitySchema;
  List<String> filteredFields;
  Boolean modularEntity;

  public SqlGenerator(String qualifiedTable, Query rootQuery, String version, Boolean filesQuery) throws IOException {
    this.qualifiedTable = qualifiedTable;
    this.rootQuery = rootQuery;
    this.version = version;
    this.filesQuery = filesQuery;
    int dotPos = qualifiedTable.lastIndexOf('.');
    this.project = dotPos == -1 ? "" : qualifiedTable.substring(0, dotPos);
    this.table = dotPos == -1 ? qualifiedTable : qualifiedTable.substring(dotPos + 1);
    this.fileTable =
        dotPos == -1
            ? qualifiedTable.replace("Subjects", TableSchema.FILES_COLUMN)
            : qualifiedTable.substring(dotPos + 1).replace("Subjects", TableSchema.FILES_COLUMN);
    this.tableSchema = TableSchema.getSchema(version);
    this.fileTableSchema = TableSchema.getSchema(fileTable);
    this.tableSchemaMap = TableSchema.buildSchemaMap(this.tableSchema);
    this.fileTableSchemaMap = TableSchema.buildSchemaMap(this.fileTableSchema);

    initializeEntityFields();
  }

  protected void initializeEntityFields() {
    QueryGenerator queryGenerator = this.getClass().getAnnotation(QueryGenerator.class);
    this.modularEntity = queryGenerator != null;
    this.entitySchema =
        queryGenerator != null
            ? TableSchema.getDefinitionByName(tableSchema, queryGenerator.Entity())
            : new EntitySchema();

    this.entitySchema.setTable(table);

    this.filteredFields =
        queryGenerator != null ? Arrays.asList(queryGenerator.ExcludedFields()) : List.of();
  }

  protected QueryContext buildQueryContext(EntitySchema entitySchema, Boolean filesQuery, Boolean subQuery) {
    return new QueryContext(tableSchemaMap, qualifiedTable, table, project, fileTable, fileTableSchemaMap)
              .setFilesQuery(filesQuery)
              .setEntitySchema(entitySchema)
              .setIncludeSelect(!subQuery);
  }

  public String generate() throws IllegalArgumentException {
    return sql(qualifiedTable, rootQuery, false);
  }

  protected String sql(String tableOrSubClause, Query query, Boolean subQuery)
      throws IllegalArgumentException {
    return SqlTemplate.resultsWrapper(
            resultsQuery(
                    query,
                    tableOrSubClause,
                    subQuery,
                    buildQueryContext(this.entitySchema, filesQuery, subQuery)));
  }

  protected String resultsQuery(
      Query query, String tableOrSubClause, Boolean subQuery, QueryContext ctx) {
    if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
      // A SUBQUERY is built differently from other queries. The FROM clause is the
      // SQL version of
      // the right subtree, instead of using table. The left subtree is now the top
      // level query.
      return resultsQuery(
          query.getL(),
          String.format("(%s)", sql(tableOrSubClause, query.getR(), true)),
          subQuery,
          buildQueryContext(ctx.getEntitySchema(), filesQuery, subQuery));
    }

    EntitySchema schema = ctx.getEntitySchema();

    String[] filesParts =
        Stream.concat(schema.getPartsStream(), Stream.of(TableSchema.FILES_COLUMN)).toArray(String[]::new);
    String filesAlias = SqlUtil.getAlias(filesParts.length - 1, filesParts);

    Stream<Unnest> filesUnnests =
        filesQuery
            ? SqlUtil.getUnnestsFromParts(ctx, table, filesParts, true, SqlUtil.JoinType.INNER)
            : Stream.empty();

    String condition = ((BasicOperator) query).buildQuery(ctx);
    String selectFields =
        getSelect(ctx, schema.getPrefix(), !this.modularEntity).collect(Collectors.joining(", "));

    var fromClause =
        Stream.concat(
                Stream.concat(
                    Stream.concat(
                        Stream.of(baseFromClause(tableOrSubClause)), ctx.getUnnests().stream().map(Unnest::toString)),
                        schema.getUnnests(ctx).map(Unnest::toString)),
                filesUnnests.map(Unnest::toString))
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

    return SqlTemplate.resultsQuery(
        getPartitionByFields(ctx, ctx.getFilesQuery() ? fileTable : schema.getPrefix())
            .collect(Collectors.joining(", ")),
        subQuery ? String.format("%s.*", table) : selectFields,
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

  protected Stream<String> getSelect(QueryContext ctx, String table, Boolean skipExcludes) {
    if (ctx.getSelect().size() > 0) {
      return ctx.getSelect().stream();
    } else {
      return getSelectsFromEntity(ctx, ctx.getFilesQuery() ? fileTable : table, skipExcludes);
    }
  }

  protected Stream<String> getSelectsFromEntity(
      QueryContext ctx, String prefix, Boolean skipExcludes) {
    var defaultId = String.format("%s_id", EntitySchema.DEFAULT_PATH.toLowerCase());
    ctx.addAlias(defaultId, String.format("%s.id", table));

    Stream<String> idSelects = Stream.of();
    if (entitySchema.wasFound() || ctx.getFilesQuery()) {
      var path = entitySchema.getPath();
      idSelects =
          Stream.concat(
              Stream.of(String.format("%s.id AS %s", table, defaultId)),
              SqlUtil.getIdSelectsFromPath(ctx, path, entitySchema.wasFound() && ctx.getFilesQuery()));

      var parts = entitySchema.getParts();
      ctx.addPartitions(Stream.of(String.format("%s.id", table)));
      ctx.addPartitions(
          IntStream.range(0, parts.length)
              .mapToObj(i -> String.format("%s.id", SqlUtil.getAlias(i, parts))));
    }
    return combinedSelects(ctx, prefix, skipExcludes, idSelects);
  }

  protected Stream<String> combinedSelects(
      QueryContext ctx, String prefix, Boolean skipExcludes, Stream<String> idSelects) {
    return Stream.concat(
        (ctx.getFilesQuery()
                ? fileTableSchema
                : entitySchema.wasFound() ? List.of(entitySchema.getSchemaFields()) : tableSchema)
            .stream()
                .filter(
                    definition ->
                        !(ctx.getFilesQuery()
                                && List.of("ResearchSubject", "Subject", "Specimen")
                                    .contains(definition.getName()))
                            && (skipExcludes || !filteredFields.contains(definition.getName())))
                .map(
                    definition -> {
                      ctx.addAlias(
                          definition.getName(),
                          String.format(
                              "%s%s",
                              List.of(table, fileTable).contains(prefix)
                                  ? String.format("%s.", prefix)
                                  : String.format("%s.", SqlUtil.getAntiAlias(prefix)),
                              definition.getName()));
                      return String.format("%1$s.%2$s AS %2$s", prefix, definition.getName());
                    }),
        idSelects);
  }

  protected Stream<? extends Class<?>> getQueryGeneratorClasses() {
    ClassPathScanningCandidateComponentProvider scanner =
        new ClassPathScanningCandidateComponentProvider(false);

    scanner.addIncludeFilter(new AnnotationTypeFilter(QueryGenerator.class));

    return scanner.findCandidateComponents("bio.terra.cda.app.generators").stream()
        .map(
            cls -> {
              try {
                return Class.forName(cls.getBeanClassName());
              } catch (ClassNotFoundException e) {
                return null;
              }
            })
        .filter(Objects::nonNull);
  }

  protected Stream<? extends Class<?>> getFileClasses() {
    return getQueryGeneratorClasses()
        .filter(
            cls -> {
              QueryGenerator generator = cls.getAnnotation(QueryGenerator.class);
              var schema = TableSchema.getDefinitionByName(tableSchema, generator.Entity());
              TableSchema.SchemaDefinition[] fields = schema.wasFound()
                      ? schema.getSchemaFields() : tableSchema.toArray(TableSchema.SchemaDefinition[]::new);
              return Arrays.stream(fields)
                      .map(TableSchema.SchemaDefinition::getName)
                      .anyMatch(s -> s.equals(TableSchema.FILES_COLUMN));
            });
  }
}
