package bio.terra.cda.app.util;

import bio.terra.cda.app.generators.QueryGenerator;
import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.TableInfo;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import java.util.Objects;
import java.util.stream.Stream;

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

    public static Stream<? extends Class<?>> getFileClasses(DataSetInfo dataSetInfo) {
        return getQueryGeneratorClasses()
                .filter(
                        cls -> {
                            QueryGenerator generator = cls.getAnnotation(QueryGenerator.class);
                            TableInfo tableInfo = dataSetInfo.getTableInfo(generator.entity());
                            TableInfo fileTableInfo = dataSetInfo.getTableInfo(TableSchema.FILE_PREFIX);
                            return Objects.nonNull(tableInfo.getPathToTable(fileTableInfo, true))
                                    && generator.hasFiles();
                        });
    }
}
