package bio.terra.cda.app.generators;

import bio.terra.cda.app.generators.SqlGenerator;
import bio.terra.cda.generated.model.Query;

import java.io.IOException;

public class CountsSqlGenerator extends SqlGenerator {
    public CountsSqlGenerator(String qualifiedTable, Query rootQuery, String version) throws IOException {
        super(qualifiedTable, rootQuery, version);
    }

    @Override
    public String generate() {
        return sql(qualifiedTable, rootQuery);
    }

    private String sql(String tableOrSubClause, Query query) {
        if (query.getNodeType() == Query.NodeTypeEnum.SUBQUERY) {
            // A SUBQUERY is built differently from other queries. The FROM clause is the
            // SQL version of
            // the right subtree, instead of using table. The left subtree is now the top
            // level query.
            return sql(String.format("(%s)", sql(tableOrSubClause, query.getR())), query.getL());
        }

        String condition = null;
        try {
            condition = queryString(query);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return String.format(
                "SELECT\n"
                        + "  top_level_file.system,\n"
                        + "  CASE\n"
                        + "    WHEN subject_count IS NULL THEN 0\n"
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
                        + "  END AS specimen_file_count\n"
                        + "FROM\n"
                        + "  (\n"
                        + "    SELECT\n"
                        + "      _file_identifier.system,\n"
                        + "      COUNT(_file_identifier.value) AS count_file\n"
                        + "    FROM\n"
                        + "      %2$s AS all_v2_1,\n"
                        + "      UNNEST(all_v2_1.File) AS _File,\n"
                        + "      UNNEST(_File.identifier) AS _file_identifier\n"
                        + "    WHERE\n"
                        + "      %1$s\n"
                        + "    GROUP BY\n"
                        + "      _file_identifier.system\n"
                        + "  ) AS top_level_file\n"
                        + "  LEFT OUTER JOIN (\n"
                        + "    SELECT\n"
                        + "      _identifier.system,\n"
                        + "      COUNT(_identifier.value) AS count_researchsubject\n"
                        + "    FROM\n"
                        + "      %2$s AS all_v2_1,\n"
                        + "      UNNEST(ResearchSubject) AS _ResearchSubject,\n"
                        + "      UNNEST(_ResearchSubject.identifier) AS _identifier\n"
                        + "    WHERE\n"
                        + "      %1$s\n"
                        + "    GROUP BY\n"
                        + "      _identifier.system\n"
                        + "  ) AS researchsubject_count ON researchsubject_count.system = top_level_file.system\n"
                        + "  LEFT OUTER JOIN (\n"
                        + "    SELECT\n"
                        + "      _file_identifier.system,\n"
                        + "      COUNT(_file_identifier.value) AS count_researchsubject\n"
                        + "    FROM\n"
                        + "      %2$s AS all_v2_1,\n"
                        + "      UNNEST(ResearchSubject) AS _ResearchSubject,\n"
                        + "      UNNEST(_ResearchSubject.File) AS _researchsubject_file,\n"
                        + "      UNNEST(_researchsubject_file.identifier) AS _file_identifier\n"
                        + "    WHERE\n"
                        + "      %1$s\n"
                        + "    GROUP BY\n"
                        + "      _file_identifier.system\n"
                        + "  ) AS researchsubject_file_count ON researchsubject_file_count.system = top_level_file.system\n"
                        + "  LEFT OUTER JOIN (\n"
                        + "    SELECT\n"
                        + "      _identifier.system,\n"
                        + "      COUNT(_identifier.value) AS count_specimen\n"
                        + "    FROM\n"
                        + "      %2$s AS all_v2_1,\n"
                        + "      UNNEST(ResearchSubject) AS _ResearchSubject,\n"
                        + "      UNNEST(_ResearchSubject.Specimen) AS _Specimen,\n"
                        + "      UNNEST(_Specimen.identifier) AS _identifier\n"
                        + "    WHERE\n"
                        + "      %1$s\n"
                        + "    GROUP BY\n"
                        + "      _identifier.system\n"
                        + "  ) AS specimen_count ON specimen_count.system = top_level_file.system\n"
                        + "  LEFT OUTER JOIN (\n"
                        + "    SELECT\n"
                        + "      _file_identifier.system,\n"
                        + "      COUNT(_file_identifier.value) AS count_file\n"
                        + "    FROM\n"
                        + "      %2$s AS all_v2_1,\n"
                        + "      UNNEST(ResearchSubject) AS _ResearchSubject,\n"
                        + "      UNNEST(_ResearchSubject.Specimen) AS _Specimen,\n"
                        + "      UNNEST(_Specimen.File) AS _File,\n"
                        + "      UNNEST(_File.identifier) AS _file_identifier\n"
                        + "    WHERE\n"
                        + "      %1$s\n"
                        + "    GROUP BY\n"
                        + "      _file_identifier.system\n"
                        + "  ) AS specimen_file_count ON specimen_file_count.system = top_level_file.system\n"
                        + "  LEFT OUTER JOIN (\n"
                        + "    SELECT\n"
                        + "      _identifier.system,\n"
                        + "      COUNT(_identifier.value) AS count_file\n"
                        + "    FROM\n"
                        + "      %2$s AS all_v2_1,\n"
                        + "      UNNEST(identifier) AS _identifier\n"
                        + "    WHERE\n"
                        + "      %1$s\n"
                        + "    GROUP BY\n"
                        + "      _identifier.system\n"
                        + "  ) AS subject_count ON subject_count.system = top_level_file.system",
                condition, qualifiedTable);
    }
}
