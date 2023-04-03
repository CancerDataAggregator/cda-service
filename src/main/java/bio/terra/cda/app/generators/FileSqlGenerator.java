package bio.terra.cda.app.generators;

import bio.terra.cda.app.models.ForeignKey;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.models.TableRelationship;
import bio.terra.cda.app.models.Unnest;
import bio.terra.cda.app.util.EndpointUtil;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlTemplate;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import com.google.cloud.bigquery.Field;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileSqlGenerator extends SqlGenerator {
    private List<TableInfo> tableInfoList;
    private boolean startsWithFile;
    private HashMap<String, Unnest> idUnnests;

    public FileSqlGenerator(String qualifiedTable, Query rootQuery, String version)
            throws IOException {
        super(qualifiedTable, rootQuery, version, true);

        idUnnests = new HashMap<>();
    }

    @Override
    protected void preInit() {
        tableInfoList = getTableInfosAsSortedList();

        TableInfo fileTableInfo = this.dataSetInfo.getTableInfo(TableSchema.FILE_PREFIX);

        boolean hasPathToAll = true;
        for (TableInfo ti : tableInfoList) {
            if (Objects.isNull(fileTableInfo.getPathToTable(ti))) {
                hasPathToAll = false;
            }
        }

        startsWithFile = hasPathToAll;
    }

    @Override
    protected void initializeEntityFields() {
        QueryGenerator queryGenerator = this.getClass().getAnnotation(QueryGenerator.class);
        this.modularEntity = queryGenerator != null;

        this.entityTable = startsWithFile ? this.dataSetInfo.getTableInfo(TableSchema.FILE_PREFIX)
                : queryGenerator != null ? this.dataSetInfo.getTableInfo(queryGenerator.entity())
                        : this.dataSetInfo.getTableInfo(version);

        this.filteredFields = Arrays.stream(this.entityTable.getSchemaDefinitions())
                .filter(schemaDefinition -> !schemaDefinition.isExcludeFromSelect()
                        || (startsWithFile && Objects.nonNull(schemaDefinition.getForeignKeys())
                                && schemaDefinition.getForeignKeys().length > 0))
                .map(TableSchema.SchemaDefinition::getName).collect(Collectors.toList());
    }

    @Override
    protected String sql(String tableOrSubClause, Query query, boolean subQuery,
            boolean hasSubClause, boolean ignoreWith)
            throws UncheckedExecutionException, IllegalArgumentException {
        return startsWithFile
                ? startsWithFileQuery(tableOrSubClause, query, subQuery, hasSubClause, ignoreWith)
                : startsWithEntityQuery(tableOrSubClause, query, subQuery, hasSubClause,
                        ignoreWith);
    }

    private String startsWithFileQuery(String tableOrSubClause, Query query, boolean subQuery,
            boolean hasSubClause, boolean ignoreWith) throws IllegalArgumentException {
        QueryContext ctx = buildQueryContext(this.entityTable, filesQuery, subQuery);

        Arrays.stream(this.entityTable.getSchemaDefinitions())
                .filter(schemaDefinition -> Objects.nonNull(schemaDefinition.getForeignKeys())
                        && schemaDefinition.getForeignKeys().length > 0
                        && schemaDefinition.getMode().equals(Field.Mode.REPEATED.toString()))
                .forEach(schemaDefinition -> {
                    List<Unnest> unnestList =
                            this.unnestBuilder
                                    .fromRelationshipPath(
                                            this.entityTable.getPathToTable(
                                                    this.dataSetInfo.getTableInfoFromField(
                                                            schemaDefinition.getName())),
                                            SqlUtil.JoinType.LEFT, true)
                                    .collect(Collectors.toList());
                    Arrays.stream(schemaDefinition.getForeignKeys()).forEach(foreignKey -> {
                        String location = foreignKey.getLocation();
                        if (location.length() > 0) {
                            String[] locationSplit = location.split("\\.");
                            this.unnestBuilder
                                    .addAdditionalJoinPath(locationSplit[locationSplit.length - 1],
                                            this.dataSetInfo
                                                    .getTableInfo(schemaDefinition.getName())
                                                    .getPartitionKeyAlias(this.dataSetInfo));
                        }
                    });
                    ctx.addUnnests(unnestList.stream());
                });

        String results = SqlTemplate
                .resultsWrapper(resultsQuery(query, tableOrSubClause, subQuery, ctx, hasSubClause));

        String withStatement = "";
        if (this.viewListBuilder.hasAny() && !ignoreWith) {
            withStatement = getWithStatement();
        }

        return String.format("%s%s", withStatement, results);
    }

    private String startsWithEntityQuery(String tableOrSubClause, Query query, boolean subQuery,
            boolean hasSubClause, boolean ignoreWith) {
        StringBuilder sb = new StringBuilder();
        AtomicReference<String> previousAlias = new AtomicReference<>("");
        List<String> tables = new ArrayList<>();

        tableInfoList.forEach(tableInfo -> {
            var resultsQuery = resultsQuery(query, tableOrSubClause, subQuery,
                    buildQueryContext(tableInfo, true, subQuery), false);
            var resultsAlias = String.format("%s_files",
                    tableInfo.getAdjustedTableName().toLowerCase(Locale.ROOT));

            TableRelationship[] tablePath = tableInfo.getTablePath();
            List<String> aliases = Arrays.stream(tablePath)
                    .map(tableRelationship -> tableRelationship.getFromTableInfo()
                            .getPartitionKeyFullName(this.dataSetInfo))
                    .collect(Collectors.toList());

            aliases.add(tableInfo.getPartitionKeyFullName(this.dataSetInfo));

            sb.append(String.format("%1$s as (%2$s),", String.format("%s", resultsAlias),
                    String.format("%s%s", SqlTemplate.resultsWrapper(resultsQuery),
                            previousAlias.get().equals("") ? ""
                                    : String.format(
                                            " AND CONCAT(results.file_id, %1$s) not in (SELECT CONCAT(%2$s.file_id, %3$s) FROM %2$s)",
                                            aliases.stream()
                                                    .map(a -> String.format(
                                                            SqlUtil.ALIAS_FIELD_FORMAT, "results",
                                                            a))
                                                    .collect(Collectors.joining(", ")),
                                            previousAlias,
                                            aliases.stream()
                                                    .map(a -> String.format(
                                                            SqlUtil.ALIAS_FIELD_FORMAT,
                                                            previousAlias, a))
                                                    .collect(Collectors.joining(", "))))));
            previousAlias.set(resultsAlias);
            tables.add(resultsAlias);
        });

        sb.append(String.format("unioned_result as (%s) ",
                tables.stream().map(alias -> String.format("SELECT %1$s.* FROM %1$s", alias))
                        .collect(Collectors.joining(" UNION ALL "))));

        sb.append("SELECT unioned_result.* FROM unioned_result");

        StringBuilder newSb = new StringBuilder();
        String withStatement = "WITH ";
        if (this.viewListBuilder.hasAny() && !ignoreWith) {
            withStatement = String.format("%s,", getWithStatement());
        }

        newSb.append(withStatement);
        newSb.append(sb.toString());

        return newSb.toString();
    }

    @Override
    protected Stream<String> getSelectsFromEntity(QueryContext ctx, String prefix,
            boolean skipExcludes) {

        List<String> idSelects =
                startsWithFile ? buildIdSelectsStartsWithFile(ctx) : buildIdSelects(ctx);

        return combinedSelects(ctx, prefix, true, idSelects.stream().distinct());
    }

    private List<String> buildIdSelects(QueryContext ctx) {
        List<String> idSelects = new ArrayList<>();

        tableInfoList.forEach(tableInfo -> {
            var pathParts = tableInfo.getTablePath();
            var realParts = ctx.getTableInfo().getTablePath();
            String value = realParts.length < pathParts.length ? "''"
                    : tableInfo.getPartitionKeyAlias(this.dataSetInfo);

            idSelects.add(String.format("%s AS %s", value,
                    tableInfo.getPartitionKeyFullName(this.dataSetInfo)));

            if (realParts.length == 0) {
                ctx.addPartitions(
                        Stream.of(this.partitionBuilder.of(ctx.getTableInfo().getTableName(),
                                ctx.getTableInfo().getPartitionKeyAlias(this.dataSetInfo))));
            } else {
                ctx.addPartitions(this.partitionBuilder.fromRelationshipPath(realParts));
            }
        });

        return idSelects;
    }

    private List<String> buildIdSelectsStartsWithFile(QueryContext ctx) {
        List<String> idSelects = new ArrayList<>();

        Arrays.stream(this.entityTable.getSchemaDefinitions())
                .filter(schemaDefinition -> Objects.nonNull(schemaDefinition.getForeignKeys())
                        && schemaDefinition.getForeignKeys().length > 0
                        && schemaDefinition.getMode().equals(Field.Mode.REPEATED.toString()))
                .forEach(schemaDefinition -> {
                    TableInfo tableInfo = this.dataSetInfo.getTableInfo(schemaDefinition.getName());

                    ForeignKey foreignKey = schemaDefinition.getForeignKeys()[0];
                    String[] locationSplit = foreignKey.getLocation().split("\\.");

                    String tableAlias = foreignKey.getLocation().length() == 0
                            ? this.dataSetInfo.getKnownAliases().get(foreignKey.getTableName())
                            : locationSplit[locationSplit.length - 1];

                    TableInfo destinationTable = this.dataSetInfo.getTableInfo(tableAlias);
                    TableRelationship[] path = tableInfo.getPathToTable(destinationTable, false);
                    ctx.addUnnests(this.unnestBuilder.fromRelationshipPath(path,
                            SqlUtil.JoinType.LEFT, false));
                    ctx.addPartitions(
                            Stream.of(this.partitionBuilder.of(destinationTable.getTableName(),
                                    destinationTable.getPartitionKeyAlias(this.dataSetInfo))));
                    idSelects.add(String.format("%s as %s",
                            destinationTable.getPartitionKeyAlias(this.dataSetInfo),
                            destinationTable.getPartitionKeyFullName(this.dataSetInfo)));
                });

        return idSelects;
    }

    private List<TableInfo> getTableInfosAsSortedList() {
        return EndpointUtil.getFileClasses(this.dataSetInfo).map(clazz -> {
            var annotation = clazz.getAnnotation(QueryGenerator.class);
            return this.dataSetInfo.getTableInfo(annotation.entity());
        }).sorted((table1, table2) -> {
            var firstSplit = table1.getTablePath();
            var secondSplit = table2.getTablePath();

            return Integer.compare(secondSplit.length, firstSplit.length);
        }).collect(Collectors.toList());
    }
}
