package bio.terra.cda.app.generators;

import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlUtil;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FileSqlGenerator extends SqlGenerator {
  public FileSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version);
  }

  @Override
  protected Stream<String> getSelectsFromEntity(
      QueryContext ctx, String prefix, Boolean skipExcludes) {
    ctx.addAlias("subject_id", String.format("%s.id", table));

    List<String> idSelects = new ArrayList<>();
    getFileClasses()
        .forEach(
            clazz -> {
              var annotation = clazz.getClass().getAnnotation(QueryGenerator.class);
              var entitySchema = TableSchema.getDefinitionByName(tableSchema, annotation.Entity());
              String path = entitySchema != null ? entitySchema.x() : "Subject";

              var pathParts = path.split("\\.");

              idSelects.addAll(
                  path.equals("Subject")
                      ? List.of(String.format("%s.id as subject_id", table))
                      : SqlUtil.getIdSelectsFromPath(ctx, path, true).collect(Collectors.toList()));

              if (!path.equals("Subject")) {
                ctx.addUnnests(SqlUtil.getUnnestsFromParts(ctx, table, pathParts, true));
                ctx.addPartitions(
                    IntStream.range(0, pathParts.length)
                        .mapToObj(i -> String.format("%s.id", SqlUtil.getAlias(i, pathParts))));
              } else {
                ctx.addPartitions(Stream.of(String.format("%s.id", table)));
              }
            });

    return combinedSelects(ctx, prefix, skipExcludes, idSelects.stream().distinct());
  }
}
