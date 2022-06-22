package bio.terra.cda.app.operators;

import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Objects;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

public class OperatorDeserializer extends JsonDeserializer<Query> {
  @Override
  public Query deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    // Get reference to ObjectCodec
    ObjectCodec codec = p.getCodec();

    // Parse "object" node into Jackson's tree model
    JsonNode node = codec.readTree(p);

    if (node.isNull()) {
      return null;
    }

    var nodeType = node.get("node_type");

    Query.NodeTypeEnum type = Query.NodeTypeEnum.fromValue(nodeType.textValue());

    ClassPathScanningCandidateComponentProvider scanner =
        new ClassPathScanningCandidateComponentProvider(false);

    scanner.addIncludeFilter(new AnnotationTypeFilter(QueryOperator.class));

    var clazz =
        scanner.findCandidateComponents("bio.terra.cda.app.operators").stream()
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
                  QueryOperator operator = cls.getAnnotation(QueryOperator.class);
                  return Arrays.asList(operator.nodeType()).contains(type);
                })
            .findFirst();

    Query query;

    if (clazz.isPresent()) {
      Constructor<?> ctor = null;
      try {
        ctor = clazz.get().getConstructor();
      } catch (NoSuchMethodException e) {
        return null;
      }
      try {
        query = (Query) ctor.newInstance();
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        return null;
      }
    } else {
      query = new BasicOperator();
    }

    query.setNodeType(type);
    query.setL(codec.treeToValue(node.get("l"), Query.class));

    Query left = query.getL();
    if (Objects.nonNull(left)) {
      ((BasicOperator)left).setParent((BasicOperator) query);
    }

    query.setR(codec.treeToValue(node.get("r"), Query.class));

    Query right = query.getR();
    if (Objects.nonNull(right)) {
      ((BasicOperator)right).setParent((BasicOperator) query);
    }

    query.setValue(node.hasNonNull("value") ? node.get("value").textValue() : null);

    return query;
  }
}
