package bio.terra.cda.app.generators;

import bio.terra.cda.app.operators.Select;
import bio.terra.cda.app.operators.SelectValues;
import bio.terra.cda.app.util.EntitySchema;
import bio.terra.cda.app.util.QueryUtil;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import com.google.cloud.Tuple;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CountsSqlGenerator extends SqlGenerator {
  public CountsSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version);
  }

  @Override
  protected String sql(String tableOrSubClause, Query query, Boolean subQuery, Boolean filesQuery)
      throws UncheckedExecutionException, IllegalArgumentException {
    Map<String, EntitySchema> entityMap = new HashMap<>();

    getQueryGeneratorClasses()
        .forEach(
            clazz -> {
              var annotation = clazz.getAnnotation(QueryGenerator.class);
              var entitySchema = TableSchema.getDefinitionByName(tableSchema, annotation.Entity());

              entityMap.put(annotation.Entity(), entitySchema);
            });

    // Add a select node to completely flatten out the result set
    Query newQuery =
        new Select()
            .nodeType(Query.NodeTypeEnum.SELECT)
            .l(
                new SelectValues()
                    .nodeType(Query.NodeTypeEnum.SELECTVALUES)
                    .value(
                        entityMap.keySet().stream()
                            .map(
                                key -> {
                                  var entitySchema = entityMap.get(key);
                                  String path = entitySchema.getPath();

                                  return path.equals("Subject")
                                      ? "id"
                                      : String.format("%s.id", path);
                                })
                            .collect(Collectors.joining(","))))
            .r(QueryUtil.DeSelectifyQuery(query));

    try {
      String resultsAlias = "flattened_results";
      return String.format(
          "%s SELECT %s FROM %s",
          String.format(
              "with %s as (%s)",
              resultsAlias,
              new SqlGenerator(this.qualifiedTable, newQuery, this.version).generate()),
          entityMap.keySet().stream()
              .map(
                  key -> {
                    var entitySchema = entityMap.get(key);
                    var parts =
                        entitySchema.wasFound()
                            ? Stream.concat(
                                    entitySchema.getPartsStream(), Stream.of("id"))
                                .toArray(String[]::new)
                            : new String[] {"id"};

                    return String.format(
                        "COUNT(DISTINCT %s) AS %s",
                        String.join("_", parts), String.format("%s_count", key.toLowerCase()));
                  })
              .collect(Collectors.joining(", ")),
          resultsAlias);
    } catch (IOException e) {
      throw new UncheckedExecutionException(e);
    }
  }
}
