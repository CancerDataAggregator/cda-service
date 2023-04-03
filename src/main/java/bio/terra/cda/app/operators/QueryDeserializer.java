package bio.terra.cda.app.operators;

import bio.terra.cda.generated.model.Operator;
import bio.terra.cda.generated.model.OperatorArrayInner;
import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.cloud.Tuple;

import javax.el.LambdaExpression;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;



public class QueryDeserializer extends JsonDeserializer<Query> {
    @Override
    public Query deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException, JacksonException {

        // Get reference to ObjectCodec
        ObjectCodec codec = p.getCodec();

        // Parse "object" node into Jackson's tree model
        JsonNode node = codec.readTree(p);

        if (node.isNull()) {
            return null;
        }

        JsonNode selectNode = node.get("select");
        JsonNode whereNode = node.get("where");
        JsonNode orderByNode = node.get("orderBy");
        JsonNode groupByNode = node.get("groupBy"); // Todo: Create a Groupby class

        Query query = new Query();
        query.setWhere(codec.treeToValue(whereNode, Operator.class));

        // Todo add if statment for groupby


        if (Objects.nonNull(selectNode) &&  selectNode.isArray()) {
            try {
                query.setSelect(new InnerQueryDeserializer<>().buildNodeOrColumn(
                        selectNode, codec, Select.class));
            } catch (JsonProcessingException exception) {
                throw exception;
            }
        }

        if (Objects.nonNull(orderByNode) && orderByNode.isArray()) {
            try {
                query.setOrderBy(new InnerQueryDeserializer<>().buildNodeOrColumn(
                        orderByNode, codec, OrderBy.class));
            } catch (JsonProcessingException exception) {
                throw exception;
            }
        }


        return query;
    }



    private static class InnerQueryDeserializer<T extends OperatorArrayInner> {
        /**
         * This made to create new Operators by reading json object to java class
         * 
         * @param arrayJsonNode
         * @param codec
         * @param clz
         * @return
         * 
         * @throws JsonProcessingException
         */


        public List<T> buildNodeOrColumn(JsonNode arrayJsonNode, ObjectCodec codec, Class<? extends T> clz) throws JsonProcessingException {
            List<T> operatorList = new ArrayList<>();
            for (JsonNode operator : arrayJsonNode) {
                T newOperator = null;
                try {
                    newOperator = clz.getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (Objects.nonNull(newOperator)) {
                    if (operator.hasNonNull("node_type")) {

                        ((ListOperator)newOperator).setOperator((BasicOperator) codec.treeToValue(operator, Operator.class));
                        if (operator.hasNonNull("modifier")) {
                            newOperator.setModifier(operator.get("modifier").textValue());
                        }

                        if (operator.hasNonNull("defaultValue")) {
                            newOperator.setDefaultValue(operator.get("defaultValue").textValue());
                        }

                        operatorList.add(newOperator);
                    } else {
                        BasicOperator column =
                                new Column().setValue(operator.textValue());
                        ((ListOperator) newOperator).setOperator(column);
                        operatorList.add(newOperator);
                    }


                }

            }
            return operatorList;
        }

    }
}


