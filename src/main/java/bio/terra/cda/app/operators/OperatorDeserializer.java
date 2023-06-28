package bio.terra.cda.app.operators;

import bio.terra.cda.generated.model.Operator;
import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

/*
 * OperatorDeserializer
 *
 * This class is meant to specify how to deserialize the json related to query objects
 * into a structure that makes sense for performing different operations based off of
 * node type.
 *
 * This deserializer will use the QueryOperator annotation to find the correct class
 * based off of node_type and instantiate that class as part of the query tree.
 *
 */
public class OperatorDeserializer extends JsonDeserializer<Operator> {
  @Override
  public Operator deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    // Get reference to ObjectCodec
    ObjectCodec codec = p.getCodec();

    // Parse "object" node into Jackson's tree model
    JsonNode node = codec.readTree(p);

    if (node.isNull()) {
      return null;
    }

    var nodeType = node.get("node_type");

    Operator.NodeTypeEnum type = Operator.NodeTypeEnum.fromValue(nodeType.textValue());

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

    BasicOperator operator;

    if (clazz.isPresent()) {
      Constructor<?> ctor = null;
      try {
        ctor = clazz.get().getConstructor();
      } catch (NoSuchMethodException e) {
        return null;
      }
      try {
        operator = (BasicOperator) ctor.newInstance();
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        return null;
      }
    } else {
      operator = new BasicOperator();
    }

    operator.setNodeType(type);
    operator.setL(codec.treeToValue(node.get("l"), Operator.class));

    Operator left = operator.getL();
    if (Objects.nonNull(left)) {
      ((BasicOperator) left).setParent(operator);
    }

    operator.setR(codec.treeToValue(node.get("r"), Operator.class));

    Operator right = operator.getR();
    if (Objects.nonNull(right)) {
      ((BasicOperator) right).setParent(operator);
    }

    if (node.hasNonNull("defaultValue")) {
      operator.setDefaultValue(node.get("defaultValue").textValue());
    }

    if (node.hasNonNull("content")) {
      List<BasicOperator> content = new ArrayList<>();
      var contentObjs = node.get("content");

      switch (type) {
        case COLUMN:
        case QUOTED:
        case UNQUOTED:
          if (contentObjs.isArray()) {
            try {
              operator.setValue(contentObjs.get(0).textValue());
            } catch (ArrayIndexOutOfBoundsException exception) {
              throw new IllegalArgumentException("Content cannot be empty.");
            }
          } else {
            operator.setValue(contentObjs.textValue());
          }
          break;
        default:
          for (JsonNode arrNode : contentObjs) {
            if (arrNode.hasNonNull("node_type")) {
              content.add((BasicOperator) codec.treeToValue(arrNode, Operator.class));
            } else {
              BasicOperator column =
                      new Column().setValue(arrNode.textValue());
              content.add(column);
            }
          }
          operator.setOperators(content);
          break;
      }
    }

    return operator;
  }
}
