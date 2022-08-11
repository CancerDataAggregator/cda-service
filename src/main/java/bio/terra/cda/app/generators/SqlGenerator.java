package bio.terra.cda.app.generators;

import bio.terra.cda.app.builders.ParameterBuilder;
import bio.terra.cda.app.builders.PartitionBuilder;
import bio.terra.cda.app.builders.QueryFieldBuilder;
import bio.terra.cda.app.builders.SelectBuilder;
import bio.terra.cda.app.builders.UnnestBuilder;
import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.EntitySchema;
import bio.terra.cda.app.models.Partition;
import bio.terra.cda.app.models.Select;
import bio.terra.cda.app.models.Unnest;
import bio.terra.cda.app.operators.BasicOperator;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlTemplate;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import com.google.cloud.bigquery.QueryJobConfiguration;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
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
  final DataSetInfo dataSetInfo;
  final boolean filesQuery;
  EntitySchema entitySchema;
  List<String> filteredFields;
  boolean modularEntity;
  UnnestBuilder unnestBuilder;
  SelectBuilder selectBuilder;
  QueryFieldBuilder queryFieldBuilder;
  PartitionBuilder partitionBuilder;
  ParameterBuilder parameterBuilder;

  public SqlGenerator(String qualifiedTable, Query rootQuery, String version, boolean filesQuery)
      throws IOException {
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
    this.dataSetInfo = new DataSetInfo.DataSetInfoBuilder().addTableSchema(version, this.tableSchema).build();

    initializeEntityFields();
    initializeBuilders();
  }

  public SqlGenerator(
      String qualifiedTable,
      Query rootQuery,
      String version,
      boolean filesQuery,
      ParameterBuilder parameterBuilder)
      throws IOException {
    this(qualifiedTable, rootQuery, version, filesQuery);

    this.parameterBuilder = parameterBuilder;
  }

  protected void initializeEntityFields() {
    QueryGenerator queryGenerator = this.getClass().getAnnotation(QueryGenerator.class);
    this.modularEntity = queryGenerator != null;
    this.entitySchema =
        queryGenerator != null
            ? TableSchema.getDefinitionByName(tableSchema, queryGenerator.entity())
            : new EntitySchema();

    this.entitySchema.setTable(table);

    this.filteredFields =
        queryGenerator != null ? Arrays.asList(queryGenerator.excludedFields()) : List.of();
  }

  protected void initializeBuilders() {
    this.queryFieldBuilder =
        new QueryFieldBuilder(this.dataSetInfo, table, fileTable, filesQuery);
    this.selectBuilder = new SelectBuilder(table, fileTable, this.dataSetInfo);
    this.unnestBuilder = new UnnestBuilder(table, fileTable, this.dataSetInfo, entitySchema.getParts(), project);
    this.partitionBuilder = new PartitionBuilder(fileTable, this.dataSetInfo);
    this.parameterBuilder = new ParameterBuilder(tableSchemaMap, fileTableSchemaMap, this.dataSetInfo);
  }

  protected QueryContext buildQueryContext(
      EntitySchema entitySchema, boolean filesQuery, boolean subQuery) {
    return new QueryContext(table, project)
        .setFilesQuery(filesQuery)
        .setEntitySchema(entitySchema)
        .setIncludeSelect(!subQuery)
        .setQueryFieldBuilder(queryFieldBuilder)
        .setSelectBuilder(selectBuilder)
        .setUnnestBuilder(unnestBuilder)
        .setPartitionBuilder(partitionBuilder)
        .setParameterBuilder(parameterBuilder);
  }

  public QueryJobConfiguration.Builder generate() throws IllegalArgumentException {
    String querySql = sql(qualifiedTable, rootQuery, false);
    QueryJobConfiguration.Builder queryJobConfigBuilder =
        QueryJobConfiguration.newBuilder(querySql);

    this.parameterBuilder.getParameterValueMap().forEach(queryJobConfigBuilder::addNamedParameter);
    return queryJobConfigBuilder;
  }

  protected String sql(String tableOrSubClause, Query query, boolean subQuery)
      throws IllegalArgumentException {
    return SqlTemplate.resultsWrapper(
        resultsQuery(
            query,
            tableOrSubClause,
            subQuery,
            buildQueryContext(this.entitySchema, filesQuery, subQuery)));
  }

  protected String resultsQuery(
      Query query, String tableOrSubClause, boolean subQuery, QueryContext ctx) {
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
        Stream.concat(schema.getPartsStream(), Stream.of(TableSchema.FILES_COLUMN))
            .toArray(String[]::new);
    String filesAlias = SqlUtil.getAlias(filesParts.length - 1, filesParts);

    ctx.addUnnests(
        filesQuery
            ? this.unnestBuilder.fromParts(table, filesParts, true, SqlUtil.JoinType.INNER)
            : this.unnestBuilder.fromParts(table, schema.getParts(), true, SqlUtil.JoinType.INNER));
    if (filesQuery) {
      ctx.addUnnests(Stream.of(this.unnestBuilder.fileJoin(SqlUtil.JoinType.INNER, filesAlias)));
    }

    String condition = ((BasicOperator) query).buildQuery(ctx);
    String selectFields =
        getSelect(ctx, schema.getPrefix(), !this.modularEntity).collect(Collectors.joining(", "));

    var fromClause =
        Stream.concat(
            Stream.of(baseFromClause(tableOrSubClause)),
            ctx.getUnnests().stream().map(Unnest::toString));

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
    return Stream.concat(
            Stream.of(String.format("%s.id", alias)),
            ctx.getPartitions().stream().map(Partition::toString))
        .distinct();
  }

  protected Stream<String> getSelect(QueryContext ctx, String table, boolean skipExcludes) {
    if (!ctx.getSelect().isEmpty()) {
      return ctx.getSelect().stream().map(Select::toString);
    } else {
      return getSelectsFromEntity(ctx, ctx.getFilesQuery() ? fileTable : table, skipExcludes);
    }
  }

  protected Stream<String> getSelectsFromEntity(
      QueryContext ctx, String prefix, boolean skipExcludes) {
    var defaultId = String.format("%s_id", EntitySchema.DEFAULT_PATH.toLowerCase());

    Stream<String> idSelects = Stream.of();
    if (entitySchema.wasFound() || ctx.getFilesQuery()) {
      var path = entitySchema.getPath();
      idSelects =
          Stream.concat(
              Stream.of(String.format("%s.id AS %s", table, defaultId)),
              SqlUtil.getIdSelectsFromPath(path, entitySchema.wasFound() && ctx.getFilesQuery()));

      var parts = entitySchema.getParts();
      ctx.addPartitions(Stream.of(this.partitionBuilder.of("id", String.format("%s.id", table))));
      ctx.addPartitions(this.partitionBuilder.fromParts(parts, tableSchemaMap));
    }
    return combinedSelects(ctx, prefix, skipExcludes, idSelects);
  }

  protected Stream<String> combinedSelects(
      QueryContext ctx, String prefix, boolean skipExcludes, Stream<String> idSelects) {

    List<TableSchema.SchemaDefinition> schema =
        entitySchema.wasFound() ? List.of(entitySchema.getSchemaFields()) : tableSchema;

    return Stream.concat(
        (ctx.getFilesQuery() ? fileTableSchema : schema)
            .stream()
                .filter(
                    definition ->
                        !(ctx.getFilesQuery()
                                && List.of("ResearchSubject", "Subject", "Specimen")
                                    .contains(definition.getName()))
                            && (skipExcludes || !filteredFields.contains(definition.getName())))
                .map(
                    definition -> String.format("%1$s.%2$s AS %2$s", prefix, definition.getName())),
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
              var schema = TableSchema.getDefinitionByName(tableSchema, generator.entity());
              TableSchema.SchemaDefinition[] fields =
                  schema.wasFound()
                      ? schema.getSchemaFields()
                      : tableSchema.toArray(TableSchema.SchemaDefinition[]::new);
              return Arrays.stream(fields)
                  .map(TableSchema.SchemaDefinition::getName)
                  .anyMatch(s -> s.equals(TableSchema.FILES_COLUMN));
            });
  }
}
