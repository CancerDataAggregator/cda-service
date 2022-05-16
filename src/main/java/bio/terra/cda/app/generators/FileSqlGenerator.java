package bio.terra.cda.app.generators;

import bio.terra.cda.app.operators.BasicOperator;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.TableSchema;
import bio.terra.cda.generated.model.Query;
import com.google.cloud.Tuple;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileSqlGenerator extends SqlGenerator {
  public FileSqlGenerator(String qualifiedTable, Query rootQuery, String version)
      throws IOException {
    super(qualifiedTable, rootQuery, version);
  }

  @Override
  protected String sql(String tableOrSubClause, Query query, Boolean subQuery, Boolean filesQuery, Boolean globalQuery)
          throws IllegalArgumentException {
    Stream<String> entityFileSql = getFileClasses().map(sqlGenerator -> sqlGenerator.generateFiles(true));

    QueryContext ctx =
            new QueryContext(
                    tableSchemaMap, tableOrSubClause, table, project, fileTable, fileTableSchemaMap)
                    .setFilesQuery(true)
                    .setIncludeSelect(!subQuery);
    ((BasicOperator) query).buildQuery(ctx);

    return String.format("SELECT results.* EXCEPT(rn) FROM (SELECT ROW_NUMBER() OVER (PARTITION BY %s) as rn, "
            + "results.* FROM (%s) as results) as results where rn = 1",
            getPartitionByFields(ctx, "results").collect(Collectors.joining(", ")),
            entityFileSql.collect(Collectors.joining(" UNION ALL ")));
  }

  @Override
  protected Stream<String> getPartitionByFields(QueryContext ctx, String alias) {
    return ctx.getSelect().size() > 0
      ? ctx.getSelect().stream().map(select -> {
          var split = select.split(" AS ");
          return String.format("%s.%s", alias, split[1]);
        })
      : Stream.of(String.format("%s.id", alias));
  }

  private Stream<SqlGenerator> getFileClasses() {
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
                    .filter(Objects::nonNull)
                    .filter(
                            cls -> {
                              QueryGenerator generator = cls.getAnnotation(QueryGenerator.class);
                              var schema = TableSchema.getDefinitionByName(tableSchema, generator.Entity());
                              if (schema == null && generator.Entity().equals("Subject")) {
                                  var schemaDef = new TableSchema.SchemaDefinition();
                                  schemaDef.setFields(tableSchema.toArray(TableSchema.SchemaDefinition[]::new));
                                  schema = Tuple.of("Subject", schemaDef);
                              }
                              return schema != null && schema.y() != null
                                      && Arrays.stream(schema.y().getFields()).map(TableSchema.SchemaDefinition::getName)
                                      .anyMatch(s -> s.equals("Files"));
                            })
                    .map(cls -> {
                      Constructor<?> ctor = null;
                      try {
                        ctor = cls.getConstructor(String.class, Query.class, String.class);
                      } catch (NoSuchMethodException e) {
                        return null;
                      }
                      try {
                        return (SqlGenerator) ctor.newInstance(this.qualifiedTable, this.rootQuery, this.version);
                      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                        return null;
                      }
                    }).filter(Objects::nonNull);
  }
}
