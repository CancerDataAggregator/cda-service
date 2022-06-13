package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.QueryField;
import bio.terra.cda.app.models.Unnest;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import com.google.cloud.bigquery.Field;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class UnnestBuilder {
    private final String table;
    private final String fileTable;
    private final String[] entityParts;
    private final String project;
    private final Boolean filesQuery;

    public UnnestBuilder(String table, String fileTable, String[] entityParts, String project, Boolean filesQuery) {
        this.table = table;
        this.fileTable = fileTable;
        this.entityParts = entityParts;
        this.project = project;
        this.filesQuery = filesQuery;
    }

    public Unnest of(
            SqlUtil.JoinType joinType,
            String path,
            String alias,
            Boolean isJoin,
            String firstJoinPath,
            String secondJoinPath) {
        return new Unnest(joinType, path, alias, isJoin, firstJoinPath, secondJoinPath);
    }

    public Unnest fileJoin(SqlUtil.JoinType joinType, String joinAlias) {
        return of(joinType,
                String.format("%s.%s", project, fileTable),
                fileTable,
                true,
                String.format("%s.id", fileTable),
                joinAlias);
    }

    public Stream<Unnest> fromQueryField(QueryField queryField) {
        return buildUnnestsFromQueryField(queryField, false);
    }

    public Stream<Unnest> fromQueryField(QueryField queryField, Boolean includeRepeated) {
        return buildUnnestsFromQueryField(queryField, includeRepeated);
    }

    private Stream<Unnest> buildUnnestsFromQueryField(QueryField queryField, Boolean includeRepeated) {
        if (queryField.isFileField()) {
            String[] filesParts =
                    Stream.concat(Arrays.stream(entityParts), Stream.of(TableSchema.FILES_COLUMN, TableSchema.ID_COLUMN))
                            .filter(part -> !part.isEmpty())
                            .toArray(String[]::new);
            String filesAlias = SqlUtil.getAlias(filesParts.length - 2, filesParts);

            return Stream.concat(
                    fromPartsWithEntityPath(
                            table, filesParts, false, String.join(".", filesParts)),
                    Stream.of(fileJoin(SqlUtil.JoinType.INNER, filesAlias)));
        } else {
            return fromPartsWithEntityPath(
                    table,
                    queryField.getParts(),
                    includeRepeated && (queryField.getMode().equals(Field.Mode.REPEATED.toString())),
                    String.join(".", entityParts));
        }
    }

    public Stream<Unnest> fromPartsWithEntityPath(
            String table, String[] parts, boolean includeLast, String entityPath) {
        return IntStream.range(0, parts.length - (includeLast ? 0 : 1))
                .mapToObj(
                        i -> {
                            String alias = SqlUtil.getAlias(i, parts);
                            String partsSub = Arrays.stream(parts, 0, i + 1).collect(Collectors.joining("."));

                            SqlUtil.JoinType joinType = entityPath.startsWith(partsSub) ? SqlUtil.JoinType.INNER : SqlUtil.JoinType.LEFT;

                            return i == 0
                                    ? new Unnest(joinType, String.format("%s.%s", table, parts[i]), alias)
                                    : new Unnest(joinType, String.format("%s.%s", SqlUtil.getAlias(i - 1, parts), parts[i]), alias);
                        });
    }

    public Stream<Unnest> fromParts(
            String table, String[] parts, boolean includeLast, SqlUtil.JoinType JoinType) {
        return IntStream.range(0, parts.length - (includeLast ? 0 : 1))
                .mapToObj(
                        i -> {
                            String alias = SqlUtil.getAlias(i, parts);
                            return i == 0
                                    ? new Unnest(JoinType, String.format("%s.%s", table, parts[i]), alias)
                                    : new Unnest(JoinType, String.format("%s.%s", SqlUtil.getAlias(i - 1, parts), parts[i]), alias);
                        });
    }
}
