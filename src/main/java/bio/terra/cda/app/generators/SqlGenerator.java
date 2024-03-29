package bio.terra.cda.app.generators;

import bio.terra.cda.app.builders.OrderByBuilder;
import bio.terra.cda.app.builders.ParameterBuilder;
import bio.terra.cda.app.builders.PartitionBuilder;
import bio.terra.cda.app.builders.QueryFieldBuilder;
import bio.terra.cda.app.builders.SelectBuilder;
import bio.terra.cda.app.builders.UnnestBuilder;
import bio.terra.cda.app.builders.ViewBuilder;
import bio.terra.cda.app.builders.ViewListBuilder;
import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.OrderBy;
import bio.terra.cda.app.models.Partition;
import bio.terra.cda.app.models.Select;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.models.TableRelationship;
import bio.terra.cda.app.models.Unnest;
import bio.terra.cda.app.models.View;
import bio.terra.cda.app.operators.BasicOperator;
import bio.terra.cda.app.util.*;
import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.QueryJobConfiguration;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqlGenerator {
  final String qualifiedTable;
  Query rootQuery;
  final String version;
  final String table;
  final String fileTable;
  final String project;
  final TableSchema.TableDefinition tableDefinition;
  final DataSetInfo dataSetInfo;
  final boolean filesQuery;
  List<String> filteredFields;
  boolean modularEntity;
  SelectBuilder selectBuilder;
  QueryFieldBuilder queryFieldBuilder;
  PartitionBuilder partitionBuilder;
  ParameterBuilder parameterBuilder;
  ViewListBuilder<View, ViewBuilder> viewListBuilder;
  OrderByBuilder orderByBuilder;
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
    this.tableDefinition = TableSchema.getSchema(version);
    this.dataSetInfo =
        new DataSetInfo.DataSetInfoBuilder().addTableSchema(version, this.tableDefinition).build();
    preInit();
    initializeEntityFields();
    initializeBuilders();
  }

  public SqlGenerator(
      String qualifiedTable,
      Query rootQuery,
      String version,
      boolean filesQuery,
      ParameterBuilder parameterBuilder,
      ViewListBuilder<View, ViewBuilder> viewListBuilder)
      throws IOException {
    this(qualifiedTable, rootQuery, version, filesQuery);
    this.parameterBuilder = parameterBuilder;
    this.viewListBuilder = viewListBuilder;
  }

  protected void preInit() {
    // nothing here, meant for subclasses to override
    // and add preInit tasks before EntityFields and builders are initialized
  }

  protected void initializeEntityFields() {
    QueryGenerator queryGenerator = this.getClass().getAnnotation(QueryGenerator.class);
    this.modularEntity = queryGenerator != null;

    this.entityTable =
        queryGenerator != null
            ? this.dataSetInfo.getTableInfo(queryGenerator.entity())
            : this.dataSetInfo.getTableInfo(version);

    this.filteredFields =
        Arrays.stream(this.entityTable.getSchemaDefinitions())
            .filter(TableSchema.SchemaDefinition::isExcludeFromSelect)
            .map(TableSchema.SchemaDefinition::getName)
            .collect(Collectors.toList());
  }

  protected void initializeBuilders() {
    this.queryFieldBuilder = new QueryFieldBuilder(this.dataSetInfo, filesQuery);
    this.selectBuilder = new SelectBuilder(this.dataSetInfo);
    this.viewListBuilder = new ViewListBuilder<>(ViewBuilder.class, this.dataSetInfo, this.project);
    this.partitionBuilder = new PartitionBuilder(this.dataSetInfo);
    this.parameterBuilder = new ParameterBuilder();
    this.orderByBuilder = new OrderByBuilder();
  }

  protected QueryContext buildQueryContext(
      TableInfo entityTable, boolean filesQuery, boolean subQuery) {
    return new QueryContext(table, project)
        .setFilesQuery(filesQuery)
        .setTableInfo(entityTable)
        .setIncludeSelect(!subQuery)
        .setQueryFieldBuilder(queryFieldBuilder)
        .setSelectBuilder(selectBuilder)
        .setUnnestBuilder(
            new UnnestBuilder(
                this.queryFieldBuilder,
                this.viewListBuilder,
                this.dataSetInfo,
                entityTable,
                project))
        .setPartitionBuilder(partitionBuilder)
        .setParameterBuilder(parameterBuilder)
        .setOrderByBuilder(orderByBuilder)
        .setViewListBuilder(viewListBuilder);
  }

  public QueryJobConfiguration.Builder generate()
      throws IllegalArgumentException, JsonProcessingException {

    String querySql = sql(qualifiedTable, rootQuery, false, false, false);
    QueryJobConfiguration.Builder queryJobConfigBuilder =
        QueryJobConfiguration.newBuilder(querySql);
    this.parameterBuilder.getParameterValueMap().forEach(queryJobConfigBuilder::addNamedParameter);
    return queryJobConfigBuilder;
  }

  protected String sql(
      String tableOrSubClause,
      Query query,
      boolean subQuery,
      boolean hasSubClause,
      boolean ignoreWith)
      throws IllegalArgumentException {
    TableInfo currentTable;

    if (subQuery) {
      currentTable = this.entityTable.getSuperTableInfo();
    } else {
      currentTable = this.entityTable;
    }

    QueryContext ctx = buildQueryContext(currentTable, filesQuery, subQuery);
    Query currentQuery = subQuery ? query : QueryUtil.removeLimitOROffest(query, ctx);

    String queryResult = resultsQuery(currentQuery, tableOrSubClause, subQuery, ctx, hasSubClause);
    String offsetandLimitString = this.getLimitOffsetString(ctx);

    String results =
        subQuery ? queryResult : SqlTemplate.resultsWrapper(queryResult, offsetandLimitString);

    String withStatement = "";
    if (this.viewListBuilder.hasAny() && !ignoreWith) {
      withStatement = getWithStatement();
    }

    return String.format("%s%s", withStatement, results);
  }

  protected String resultsQuery(
      Query query,
      String tableOrSubClause,
      boolean subQuery,
      QueryContext ctx,
      boolean hasSubClause) {
    TableInfo tableInfo = ctx.getTableInfo();

    TableRelationship[] pathToFile =
        tableInfo.getPathToTable(this.dataSetInfo.getTableInfo(TableSchema.FILE_PREFIX));
    TableRelationship[] entityPath = tableInfo.getTablePath();

    TableInfo startTable =
        Objects.isNull(entityPath) || entityPath.length == 0
            ? tableInfo
            : entityPath[0].getFromTableInfo();

    if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
      // A SUBQUERY is built differently from other queries. The FROM clause is the
      // SQL version of
      // the right subtree, instead of using table. The left subtree is now the top
      // level query.

      return resultsQuery(
          query.getL(),
          String.format(
              "(%s) as %s",
              sql(tableOrSubClause, query.getR(), true, hasSubClause, true),
              startTable.getTableAlias(this.dataSetInfo)),
          subQuery,
          buildQueryContext(
              ctx.getTableInfo(), filesQuery, subQuery), // added  supertable to get parent
          true);
    }
    UnnestBuilder newUnnestBuilder = ctx.getUnnestBuilder();
    ctx.addUnnests(
        newUnnestBuilder.fromRelationshipPath(entityPath, SqlUtil.JoinType.INNER, false));

    if (filesQuery) {
      ctx.addUnnests(
          newUnnestBuilder.fromRelationshipPath(pathToFile, SqlUtil.JoinType.INNER, false));
    }

    String condition = ((BasicOperator) query).buildQuery(ctx);
    String selectFields =
        subQuery
            ? ""
            : getSelect(ctx, tableInfo.getTableAlias(this.dataSetInfo), !this.modularEntity)
                .collect(Collectors.joining(", "));

    var fromClause =
        Stream.concat(
            hasSubClause
                ? Stream.of(tableOrSubClause)
                : Stream.of(
                    String.format(
                        "%s.%s AS %s",
                        project,
                        startTable.getTableName(),
                        startTable.getTableAlias(this.dataSetInfo))),
            ctx.getUnnests().stream().map(Unnest::toString));

    String fromString = fromClause.distinct().collect(Collectors.joining(" "));

    if (subQuery) {
      return SqlTemplate.regularQuery(
          String.format("%s.*", startTable.getTableAlias(this.dataSetInfo)),
          fromString,
          condition,
          ctx.getOrderBys().stream().map(OrderBy::toString).collect(Collectors.joining(", ")));
    }

    return SqlTemplate.resultsQuery(
        getPartitionByFields(ctx).collect(Collectors.joining(", ")),
        selectFields,
        fromString,
        condition,
        ctx.getOrderBys().stream().map(OrderBy::toString).collect(Collectors.joining(", ")));
  }

  protected Stream<String> getPartitionByFields(QueryContext ctx) {
    return Stream.concat(
            Stream.of(
                ctx.getFilesQuery()
                    ? this.dataSetInfo
                        .getTableInfo(TableSchema.FILE_PREFIX)
                        .getPartitionKeyAlias(this.dataSetInfo)
                    : ctx.getTableInfo().getPartitionKeyAlias(this.dataSetInfo)),
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
              ? this.dataSetInfo
                  .getTableInfo(TableSchema.FILE_PREFIX)
                  .getTableAlias(this.dataSetInfo)
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
      if (Objects.isNull(this.dataSetInfo.getSchemaDefinitionByFieldName(entityPartitionKey))) {
        entityPartitionKey =
            DataSetInfo.getNewNameForDuplicate(
                this.dataSetInfo.getKnownAliases(),
                entityPartitionKey,
                this.entityTable.getTableName());
      }

      idSelects =
          Stream.concat(
              Stream.of(
                  String.format(
                      "%s AS %s",
                      this.entityTable.getPartitionKeyAlias(this.dataSetInfo), entityPartitionKey)),
              Arrays.stream(path)
                  .map(
                      tableRelationship -> {
                        TableInfo fromTableInfo = tableRelationship.getFromTableInfo();

                        String partitionKey = fromTableInfo.getPartitionKey();
                        if (Objects.isNull(
                            this.dataSetInfo.getSchemaDefinitionByFieldName(partitionKey))) {
                          partitionKey =
                              DataSetInfo.getNewNameForDuplicate(
                                  this.dataSetInfo.getKnownAliases(),
                                  partitionKey,
                                  fromTableInfo.getTableName());
                        }

                        return String.format(
                            "%s AS %s",
                            fromTableInfo.getPartitionKeyAlias(this.dataSetInfo), partitionKey);
                      }));

      if (path.length > 0) {
        ctx.addPartitions(this.partitionBuilder.fromRelationshipPath(path));
      } else {
        ctx.addPartitions(
            Stream.of(
                this.partitionBuilder.of(
                    this.entityTable.getPartitionKey(),
                    this.entityTable.getPartitionKeyAlias(this.dataSetInfo))));
      }
    }
    return combinedSelects(ctx, prefix, skipExcludes, idSelects);
  }

  protected Stream<String> combinedSelects(
      QueryContext ctx, String prefix, boolean skipExcludes, Stream<String> idSelects) {
    TableInfo fileTableInfo = this.dataSetInfo.getTableInfo(TableSchema.FILE_PREFIX);
    List<String> fileFilteredFields =
        Arrays.stream(fileTableInfo.getSchemaDefinitions())
            .filter(TableSchema.SchemaDefinition::isExcludeFromSelect)
            .map(TableSchema.SchemaDefinition::getName)
            .collect(Collectors.toList());

    return Stream.concat(
            Arrays.stream(
                    ctx.getFilesQuery()
                        ? this.dataSetInfo
                            .getTableInfo(TableSchema.FILE_PREFIX)
                            .getSchemaDefinitions()
                        : this.entityTable.getSchemaDefinitions())
                .filter(
                    definition ->
                        !(ctx.getFilesQuery() && fileFilteredFields.contains(definition.getName()))
                            && (skipExcludes || !filteredFields.contains(definition.getName())))
                .map(
                    definition -> {
                      String fieldSelect = String.format("%s.%s", prefix, definition.getName());

                      if (definition.getMode().equals(Field.Mode.REPEATED.toString())
                          && ctx.getOrderBys().stream()
                              .anyMatch(ob -> ob.getFieldName().equals(definition.getName()))) {
                        fieldSelect =
                            this.dataSetInfo
                                .getTableInfo(definition.getName())
                                .getTableAlias(this.dataSetInfo);
                      }

                      return String.format("%1$s AS %2$s", fieldSelect, definition.getAlias());
                    }),
            idSelects)
        .distinct();
  }

  protected String getWithStatement() {
    return String.format(
        "WITH %s ",
        this.viewListBuilder.build().stream()
            .map(View::toString)
            .collect(Collectors.joining(", ")));
  }

  /**
   * This code checks if the "limit" or "offset" values are set in the "ctx" object. If they are, it
   * appends the values to a string and returns it.
   *
   * @param ctx
   * @return
   */
  protected String getLimitOffsetString(QueryContext ctx) {

    String offsetAndlimitString = "";
    if (ctx.getLimit().isPresent()) {
      offsetAndlimitString += String.format(" LIMIT %s ", ctx.getLimit().get());
    }
    if (ctx.getOffset().isPresent()) {
      offsetAndlimitString += String.format(" OFFSET %s ", ctx.getOffset().get());
    }

    return offsetAndlimitString;
  }
}
