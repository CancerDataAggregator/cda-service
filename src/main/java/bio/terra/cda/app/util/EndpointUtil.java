package bio.terra.cda.app.util;

import bio.terra.cda.app.generators.QueryGenerator;
import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.TableInfo;
import java.util.Objects;
import java.util.stream.Stream;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

public class EndpointUtil {
  private EndpointUtil() {}

  public static Stream<? extends Class<?>> getQueryGeneratorClasses() {
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

}
