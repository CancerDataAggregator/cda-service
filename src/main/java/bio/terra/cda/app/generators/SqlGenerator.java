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
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.models.TableRelationship;
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
  final List<TableSchema.SchemaDefinition> tableSchema;
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
  TableInfo entityTable;

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

    this.entityTable = queryGenerator != null
      ? this.dataSetInfo.getTableInfo(queryGenerator.entity())
      : this.dataSetInfo.getTableInfo(version);

    this.filteredFields =
        queryGenerator != null ? Arrays.asList(queryGenerator.excludedFields()) : List.of();
  }

  protected void initializeBuilders() {
    this.queryFieldBuilder =
        new QueryFieldBuilder(this.dataSetInfo, filesQuery);
    this.selectBuilder = new SelectBuilder(this.dataSetInfo);
    this.unnestBuilder = new UnnestBuilder(this.queryFieldBuilder, this.dataSetInfo, this.entityTable, project);
    this.partitionBuilder = new PartitionBuilder(this.dataSetInfo);
    this.parameterBuilder = new ParameterBuilder();
  }

  protected QueryContext buildQueryContext(TableInfo entityTable, boolean filesQuery, boolean subQuery) {
    return new QueryContext(table, project)
        .setFilesQuery(filesQuery)
        .setTableInfo(entityTable)
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
            buildQueryContext(this.entityTable, filesQuery, subQuery)));
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
          buildQueryContext(ctx.getTableInfo(), filesQuery, subQuery));
    }

    TableInfo tableInfo = ctx.getTableInfo();

    TableRelationship[] pathToFile = tableInfo.getPathToTable(this.dataSetInfo.getTableInfo(TableSchema.FILE_PREFIX));
    TableRelationship[] entityPath = tableInfo.getTablePath();

    ctx.addUnnests(this.unnestBuilder.fromRelationshipPath(entityPath, SqlUtil.JoinType.INNER, false));

    if (filesQuery) {
      ctx.addUnnests(this.unnestBuilder.fromRelationshipPath(pathToFile, SqlUtil.JoinType.INNER, false));
    }

    String condition = ((BasicOperator) query).buildQuery(ctx);
    String selectFields =
        getSelect(ctx, tableInfo.getTableAlias(), !this.modularEntity).collect(Collectors.joining(", "));

    TableInfo startTable = Objects.isNull(entityPath) || entityPath.length == 0
            ? this.entityTable
            : entityPath[0].getFromTableInfo();
    var fromClause = Stream.concat(
            Stream.of(String.format("%s.%s AS %s",
                    project,
                    startTable.getTableName(),
                    startTable.getTableAlias())),
            ctx.getUnnests().stream().map(Unnest::toString));

    String fromString = fromClause.distinct().collect(Collectors.joining(" "));

    return SqlTemplate.resultsQuery(
        getPartitionByFields(ctx).collect(Collectors.joining(", ")),
        subQuery ? String.format("%s.*", table) : selectFields,
        fromString,
        condition);
  }

  protected Stream<String> getPartitionByFields(QueryContext ctx) {
    return Stream.concat(
            Stream.of(ctx.getFilesQuery()
                    ? this.dataSetInfo.getTableInfo(TableSchema.FILE_PREFIX).getPartitionKeyAlias()
                    : ctx.getTableInfo().getPartitionKeyAlias()),
            ctx.getPartitions().stream().map(Partition::toString))
        .distinct();
  }

  protected Stream<String> getSelect(QueryContext ctx, String table, boolean skipExcludes) {
    if (!ctx.getSelect().isEmpty()) {
      return ctx.getSelect().stream().map(Select::toString);
    } else {
      return getSelectsFromEntity(
              ctx,
              ctx.getFilesQuery()
                      ? this.dataSetInfo.getTableInfo(TableSchema.FILE_PREFIX).getTableAlias()
                      : table,
              skipExcludes);
    }
  }

  protected Stream<String> getSelectsFromEntity(
      QueryContext ctx, String prefix, boolean skipExcludes) {
    Stream<String> idSelects = Stream.of();

    if (this.entityTable.getType().equals(TableInfo.TableInfoTypeEnum.NESTED)
            || ctx.getFilesQuery()) {
      var path = this.entityTable.getTablePath();

      String entityPartitionKey = this.entityTable.getPartitionKey();
      if (Objects.isNull(this.dataSetInfo.getSchemaDefinitionByFieldName(entityPartitionKey))){
        entityPartitionKey = DataSetInfo.getNewNameForDuplicate(entityPartitionKey, this.entityTable.getTableName());
      }

      idSelects = Stream.concat(
              Stream.of(
                      String.format("%s AS %s",
                              this.entityTable.getPartitionKeyAlias(),
                              entityPartitionKey)),
              Arrays.stream(path).map(tableRelationship -> {
                  TableInfo fromTableInfo = tableRelationship.getFromTableInfo();

                  String partitionKey = fromTableInfo.getPartitionKey();
                  if (Objects.isNull(this.dataSetInfo.getSchemaDefinitionByFieldName(partitionKey))){
                    partitionKey = DataSetInfo.getNewNameForDuplicate(partitionKey, fromTableInfo.getTableName());
                  }

                  return String.format("%s AS %s",
                          fromTableInfo.getPartitionKeyAlias(),
                          partitionKey);
              }));

      if (path.length > 0) {
        ctx.addPartitions(this.partitionBuilder.fromRelationshipPath(path));
      } else {
        ctx.addPartitions(
                Stream.of(
                        this.partitionBuilder.of(
                                this.entityTable.getPartitionKey(),
                                this.entityTable.getPartitionKeyAlias())));
      }
    }
    return combinedSelects(ctx, prefix, skipExcludes, idSelects);
  }

  protected Stream<String> combinedSelects(
      QueryContext ctx, String prefix, boolean skipExcludes, Stream<String> idSelects) {
    return Stream.concat(
        Arrays.stream(ctx.getFilesQuery()
                ? this.dataSetInfo.getTableInfo(TableSchema.FILE_PREFIX).getSchemaDefinitions()
                : this.entityTable.getSchemaDefinitions())
                .filter(
                    definition ->
                        !(ctx.getFilesQuery()
                                && List.of("ResearchSubject", "Subject", "Specimen")
                                    .contains(definition.getName()))
                            && (skipExcludes || !filteredFields.contains(definition.getName())))
                .map(
                    definition -> String.format("%1$s.%2$s AS %3$s", prefix, definition.getName(), definition.getAlias())),
        idSelects).distinct();
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
