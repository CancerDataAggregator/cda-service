package bio.terra.cda.app.generators;

import bio.terra.cda.app.generators.SqlGenerator;
import bio.terra.cda.generated.model.Query;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CountsSqlGenerator extends SqlGenerator {
        public CountsSqlGenerator(String qualifiedTable, Query rootQuery, String version) throws IOException {
                super(qualifiedTable, rootQuery, version);
        }

        @Override
        public String generate() {
                return sql(qualifiedTable, rootQuery);
        }

        private Stream<String> getUnnestsFromParts(String[] parts, Boolean includeLast) {
                return IntStream.range(0, parts.length - (includeLast ? 0 : 1))
                                .mapToObj(
                                                i -> i == 0
                                                                ? String.format("UNNEST(%1$s.%2$s) AS %3$s", table,
                                                                                parts[i], getAlias(i, parts))
                                                                : String.format("UNNEST(%1$s.%2$s) AS %3$s",
                                                                                getAlias(i - 1, parts), parts[i],
                                                                                getAlias(i, parts)));
        }

        private String getAlias(Integer index, String[] parts) {
                return "_" + Arrays.stream(parts, 0, index + 1).collect(Collectors.joining("_"));
        }

        private String sql(String tableOrSubClause, Query query) {
                if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
                        // A SUBQUERY is built differently from other queries. The FROM clause is the
                        // SQL version of
                        // the right subtree, instead of using table. The left subtree is now the top
                        // level query.
                        return sql(String.format("(%s)", sql(tableOrSubClause, query.getR())), query.getL());
                }
                Supplier<Stream<String>> fromClause = () -> Stream.concat(
                                Stream.of(tableOrSubClause + " AS " + table), getUnnestColumns(query).distinct());
                String condition = null;
                try {
                        condition = queryString(query);
                } catch (Exception e) {
                        e.printStackTrace();
                }

                String where = "WHERE\n"
                                + "    %s\n";
                StringBuilder sb = new StringBuilder();
                sb.append("SELECT\n"
                                + "  identifiers.system,\n"
                                + "  CASE\n"
                                + "    WHEN subject_count.count_file IS NULL THEN 0\n"
                                + "    ELSE subject_count.count_file\n"
                                + "  END AS subject_count,\n"
                                + "  CASE\n"
                                + "    WHEN top_level_file.count_file IS NULL THEN 0\n"
                                + "    ELSE top_level_file.count_file\n"
                                + "  END AS subject_file_count,\n"
                                + "  CASE\n"
                                + "    WHEN researchsubject_count.count_researchsubject IS NULL THEN 0\n"
                                + "    ELSE researchsubject_count.count_researchsubject\n"
                                + "  END AS researchsubject_count,\n"
                                + "  CASE\n"
                                + "    WHEN researchsubject_file_count.count_researchsubject IS NULL THEN 0\n"
                                + "    ELSE researchsubject_file_count.count_researchsubject\n"
                                + "  END AS researchsubject_file_count,\n"
                                + "  CASE\n"
                                + "    WHEN specimen_count.count_specimen IS NULL THEN 0\n"
                                + "    ELSE specimen_count.count_specimen\n"
                                + "  END AS specimen_count,\n"
                                + "  CASE\n"
                                + "    WHEN specimen_file_count.count_file IS NULL THEN 0\n"
                                + "    ELSE specimen_file_count.count_file\n"
                                + "  END AS specimen_file_count \n"
                                + "FROM (\n"
                                + "    SELECT DISTINCT _Identifier.system\n"
                                + "    FROM gdc-bq-sample.integration.all_v2_1 AS all_v2_1,\n"
                                + "    UNNEST(identifier) AS _Identifier\n"
                                + ") as identifiers \n");

                var whereClause = condition != null && condition.length() > 0 ? String.format(where, condition) : "";

                var topLevelFrom = Stream.concat(
                                fromClause.get(), getUnnestsFromParts(new String[] { "File", "identifier" }, true))
                                .distinct().collect(Collectors.joining(",\n"));
                sb.append(String.format("  LEFT OUTER JOIN (\n"
                                + "    SELECT\n"
                                + "      _File_identifier.system,\n"
                                + "      COUNT(_File_identifier.value) AS count_file\n"
                                + "    FROM\n"
                                + "      %s\n"
                                + "    %s"
                                + "    GROUP BY\n"
                                + "      _File_identifier.system\n"
                                + "  ) AS top_level_file on top_level_file.system = identifiers.system\n", topLevelFrom,
                                whereClause));

                var rsFrom = Stream.concat(
                                fromClause.get(),
                                getUnnestsFromParts(new String[] { "ResearchSubject", "identifier" }, true)).distinct()
                                .collect(Collectors.joining(",\n"));
                sb.append(String.format("  LEFT OUTER JOIN (\n"
                                + "    SELECT\n"
                                + "      _ResearchSubject_identifier.system,\n"
                                + "      COUNT(_ResearchSubject_identifier.value) AS count_researchsubject\n"
                                + "    FROM\n"
                                + "      %s\n"
                                + "    %s"
                                + "    GROUP BY\n"
                                + "      _ResearchSubject_identifier.system\n"
                                + "  ) AS researchsubject_count ON researchsubject_count.system = identifiers.system\n",
                                rsFrom, whereClause));

                var rsFileFrom = Stream.concat(
                                fromClause.get(),
                                getUnnestsFromParts(new String[] { "ResearchSubject", "File", "identifier" }, true))
                                .distinct().collect(Collectors.joining(",\n"));
                sb.append(String.format("  LEFT OUTER JOIN (\n"
                                + "    SELECT\n"
                                + "      _ResearchSubject_File_identifier.system,\n"
                                + "      COUNT(_ResearchSubject_File_identifier.value) AS count_researchsubject\n"
                                + "    FROM\n"
                                + "      %s\n"
                                + "    %s"
                                + "    GROUP BY\n"
                                + "      _ResearchSubject_File_identifier.system\n"
                                + "  ) AS researchsubject_file_count ON researchsubject_file_count.system = identifiers.system\n",
                                rsFileFrom, whereClause));

                var specimenFrom = Stream.concat(
                                fromClause.get(),
                                getUnnestsFromParts(new String[] { "ResearchSubject", "Specimen", "identifier" }, true))
                                .distinct().collect(Collectors.joining(",\n"));
                sb.append(String.format("  LEFT OUTER JOIN (\n"
                                + "    SELECT\n"
                                + "      _ResearchSubject_Specimen_identifier.system,\n"
                                + "      COUNT(_ResearchSubject_Specimen_identifier.value) AS count_specimen\n"
                                + "    FROM\n"
                                + "      %s\n"
                                + "    %s"
                                + "    GROUP BY\n"
                                + "      _ResearchSubject_Specimen_identifier.system\n"
                                + "  ) AS specimen_count ON specimen_count.system = identifiers.system\n", specimenFrom,
                                whereClause));

                var specimenFileFrom = Stream.concat(
                                fromClause.get(),
                                getUnnestsFromParts(
                                                new String[] { "ResearchSubject", "Specimen", "File", "identifier" },
                                                true))
                                .distinct().collect(Collectors.joining(",\n"));
                sb.append(String.format("  LEFT OUTER JOIN (\n"
                                + "    SELECT\n"
                                + "      _ResearchSubject_Specimen_File_identifier.system,\n"
                                + "      COUNT(_ResearchSubject_Specimen_File_identifier.value) AS count_file\n"
                                + "    FROM\n"
                                + "      %s\n"
                                + "    %s"
                                + "    GROUP BY\n"
                                + "      _ResearchSubject_Specimen_File_identifier.system\n"
                                + "  ) AS specimen_file_count ON specimen_file_count.system = identifiers.system\n",
                                specimenFileFrom, whereClause));

                var identFrom = Stream.concat(
                                fromClause.get(), getUnnestsFromParts(new String[] { "identifier" }, true)).distinct()
                                .collect(Collectors.joining(",\n"));
                sb.append(String.format("  LEFT OUTER JOIN (\n"
                                + "    SELECT\n"
                                + "      _identifier.system,\n"
                                + "      COUNT(_identifier.value) AS count_file\n"
                                + "    FROM\n"
                                + "      %s\n"
                                + "    %s"
                                + "    GROUP BY\n"
                                + "      _identifier.system\n"
                                + "  ) AS subject_count ON subject_count.system = identifiers.system\n", identFrom,
                                whereClause));

                return sb.toString();
        }
}
